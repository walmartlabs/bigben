package com.walmartlabs.components.scheduler.core;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.walmart.gmp.ingestion.platform.framework.core.Hz;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmartlabs.components.core.services.Service;
import com.walmartlabs.components.scheduler.entities.Bucket;
import com.walmartlabs.components.scheduler.tasks.BulkShardTask;
import com.walmartlabs.components.scheduler.tasks.ShardStatus;
import com.walmartlabs.components.scheduler.tasks.ShardStatusList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterators.cycle;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.walmart.gmp.ingestion.platform.framework.core.ListenableFutureAdapter.adapt;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmartlabs.components.scheduler.entities.Status.ERROR;
import static com.walmartlabs.components.scheduler.entities.Status.PROCESSED;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.*;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.now;
import static java.time.ZonedDateTime.ofInstant;
import static java.util.concurrent.TimeUnit.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.tuple.Pair.of;

/**
 * Created by smalik3 on 3/8/16
 */
public class ScheduleScanner implements Service {

    public static final String BUCKET_CACHE = "bucketCache";
    public static final String EVENT_SCHEDULER = "event_scheduler";

    static {
        System.setProperty("dm.entity.packages.scan", "com.walmartlabs.components.scheduler.model");
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            System.out.println("uncaught exception: " + t);
            e.printStackTrace();
        });
    }

    @Autowired
    private transient DataManager<ZonedDateTime, Bucket> dataManager;

    @Autowired
    private Hz hz;

    private final AtomicReference<Boolean> isShutdown = new AtomicReference<>(false);

    private BucketManager bucketManager;
    private int bucketWidth;

    private static final AtomicInteger index = new AtomicInteger();
    static final ListeningScheduledExecutorService SCHEDULER =
            listeningDecorator(new ScheduledThreadPoolExecutor(getRuntime().availableProcessors(),
                    r -> new Thread(r, "InternalScheduler#" + index.getAndIncrement())));
    private static final Logger L = Logger.getLogger(ScheduleScanner.class);

    @Override
    public String name() {
        return "ScheduleScanner";
    }

    @Override
    public void init() {
        L.info("initing the event scheduler");
        int lookbackRange = PROPS.getInteger("events.backlog.check.limit", 2);
        bucketWidth = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
        final int checkpointInterval = PROPS.getInteger("event.bucket.manager.checkpoint.interval", 1);
        final TimeUnit checkpointIntervalUnits = TimeUnit.valueOf(PROPS.getProperty("event.bucket.manager.checkpoint.interval.units", MINUTES.name()));
        bucketManager = new BucketManager(lookbackRange, 2 * bucketWidth * 60, dataManager, bucketWidth * 60,
                checkpointInterval, checkpointIntervalUnits, lookbackRange, hz.hz().getMap(BUCKET_CACHE));
    }

    @Override
    public void destroy() {
        L.info("destroying the event scheduler");
    }

    @Override
    public void execute() {
        L.info("executing the EventScheduleScanner");
        final Integer scanInterval = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
        L.info("calculating the next scan bucketId");
        final ZonedDateTime now = now(UTC);
        final ZonedDateTime nextScan = nextScan(now, scanInterval);
        final long delay = ChronoUnit.MILLIS.between(now, nextScan);
        final ZonedDateTime bucket = ofInstant(ofEpochMilli(bucketize(now.toInstant().toEpochMilli(), scanInterval)), UTC);
        L.info(format("first-scan at: %s, for bucket: %s, next-scan at: %s, " +
                "initial-delay: %d ms, subsequent-scans: after every %d minutes", now, bucket, nextScan, delay, scanInterval));
        SCHEDULER.scheduleAtFixedRate(this::scan, delay, MILLISECONDS.convert(scanInterval, MINUTES), MILLISECONDS);
        L.info("executing first time scan");
        scan();
    }

    private void scan() {
        if (isShutdown.get()) {
            L.info("system is shutdown, no more schedules will be processed");
            return;
        }
        final ZonedDateTime currentBucketId = utc(bucketize(now(UTC).toInstant().toEpochMilli(), bucketWidth));
        L.info(format("scanning the schedule(s) for bucket: %s", currentBucketId));
        try {
            addCallback(bucketManager.getProcessableShardsForOrBefore(currentBucketId),
                    new FutureCallback<Multimap<ZonedDateTime, Integer>>() {
                        @Override
                        public void onSuccess(Multimap<ZonedDateTime, Integer> shards) {
                            try {
                                if (shards.isEmpty()) {
                                    L.info("nothing to schedule for bucket: " + currentBucketId);
                                    return;
                                }
                                L.info(format("%s, shards to be processed: => %s", currentBucketId, shards));
                                final List<Member> members = newArrayList(hz.hz().getCluster().getMembers());
                                final List<Entry<ZonedDateTime, Integer>> entries = newArrayList(shards.entries());
                                final Multimap<String, Pair<ZonedDateTime, Integer>> distro = TreeMultimap.create();
                                final int size = members.size();
                                for (int i = 0; i < entries.size(); i++) {
                                    final Entry<ZonedDateTime, Integer> e = entries.get(i);
                                    distro.put(members.get(i % size).getSocketAddress().toString(), of(e.getKey(), e.getValue()));
                                }
                                L.info(format("%s, schedule distribution: => " + distro, currentBucketId));

                                final int submitRetries = PROPS.getInteger("event.submit.max.retries", 10);
                                final int submitInitialDelay = PROPS.getInteger("event.submit.initial.delay", 1);
                                final int submitBackoff = PROPS.getInteger("event.submit.backoff.multiplier", 1);

                                final IExecutorService executorService = hz.hz().getExecutorService(EVENT_SCHEDULER);
                                final Iterator<Member> iterator = cycle(hz.hz().getCluster().getMembers());
                                final Map<String, Collection<Pair<ZonedDateTime, Integer>>> map = distro.asMap();

                                final ListenableFuture<List<List<ShardStatus>>> future = successfulAsList(map.entrySet().stream().map(e -> taskExecutor.async(() -> () ->
                                                transform(submitShards(executorService, iterator.next(), e.getValue(), currentBucketId), ShardStatusList::getList),
                                        "shards-submit", submitRetries, submitInitialDelay, submitBackoff, SECONDS)).collect(toList()));
                                addCallback(future,
                                        new FutureCallback<List<List<ShardStatus>>>() {
                                            @Override
                                            public void onSuccess(List<List<ShardStatus>> result) {
                                                L.info(format("schedule for bucket %s finished successfully => %s", currentBucketId, result));
                                                final List<ShardStatus> shardStatues = newArrayList(concat(result));
                                                final Set<ZonedDateTime> buckets = shardStatues.stream().map(ShardStatus::getBucketId).collect(toSet());
                                                final Set<ZonedDateTime> erroredBuckets = shardStatues.stream().filter(shardStatus -> shardStatus.getStatus() == ERROR).map(ShardStatus::getBucketId).collect(toSet());
                                                addCallback(successfulAsList(buckets.stream().map(b -> bucketManager.bucketProcessed(b, erroredBuckets.contains(b) ? ERROR : PROCESSED)).collect(toList())),
                                                        new FutureCallback<List<Bucket>>() {
                                                            @Override
                                                            public void onSuccess(List<Bucket> result) {
                                                                L.info(format("%s, buckets are marked %s: %s",
                                                                        currentBucketId, currentBucketId, result.stream().filter(b -> b != null).collect(toList())));
                                                            }

                                                            @Override
                                                            public void onFailure(Throwable t) {
                                                                L.error(format("%s, error in marking following buckets %s: %s ", currentBucketId, PROCESSED, buckets), t);
                                                            }
                                                        });
                                            }

                                            @Override
                                            public void onFailure(Throwable t) {
                                                L.error(format("schedule for bucket %s finished with error", currentBucketId), t);
                                            }
                                        });
                            } catch (Exception e) {
                                L.error("error in processing bucket: " + currentBucketId, getRootCause(e));
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            L.error("error in processing bucket: " + currentBucketId, getRootCause(t));
                        }
                    });

        } catch (Exception e) {
            L.error("error in processing bucket: " + currentBucketId, getRootCause(e));
        }
    }

    private ListenableFuture<ShardStatusList> submitShards(IExecutorService executorService, Member member, Collection<Pair<ZonedDateTime, Integer>> shardsData, ZonedDateTime bucket) {
        L.info(format("%s, submitting  for execution to member %s, shards: %s", bucket, member.getSocketAddress(), shardsData));
        bucketManager.registerForProcessing(shardsData);
        final ListenableFuture<ShardStatusList> f = adapt(executorService.submitToMember(new BulkShardTask(shardsData), member));
        addCallback(f, new FutureCallback<ShardStatusList>() {
            @Override
            public void onSuccess(ShardStatusList result) {
                L.info(format("%s, member %s reported success for shards: %s", bucket, member.getSocketAddress(), result));
                for (ShardStatus shardStatus : result.getList()) {
                    bucketManager.shardDone(shardStatus.getBucketId(), shardStatus.getShard(), shardStatus.getStatus());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                L.error(format("%s, member %s reported error for shards: %s", bucket, member.getSocketAddress(), shardsData), t);
                for (Pair<ZonedDateTime, Integer> pair : shardsData) {
                    bucketManager.shardDone(pair.getLeft(), pair.getRight(), ERROR);
                }
            }
        });
        return catching(f, Throwable.class, t -> new ShardStatusList(
                shardsData.stream().map(p -> new ShardStatus(p.getLeft(), p.getRight(), ERROR)).collect(toList())));
    }

    private final TaskExecutor taskExecutor = new TaskExecutor(newHashSet(Exception.class));

    public void shutdown() {
        isShutdown.set(true);
        bucketManager.saveCheckpoint();
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }
}
