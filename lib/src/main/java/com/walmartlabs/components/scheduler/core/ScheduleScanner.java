package com.walmartlabs.components.scheduler.core;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.walmart.gmp.ingestion.platform.framework.core.Hz;
import com.walmart.gmp.ingestion.platform.framework.core.ListenableFutureAdapter;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmartlabs.components.scheduler.model.Bucket;
import com.walmartlabs.components.scheduler.model.Bucket.BucketStatus;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.HashMultimap.create;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterators.cycle;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.ERROR;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.PROCESSED;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.*;
import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.now;
import static java.time.ZonedDateTime.ofInstant;
import static java.util.concurrent.TimeUnit.*;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.tuple.Pair.of;

/**
 * Created by smalik3 on 3/8/16
 */
public class ScheduleScanner implements Service {

    static final String BUCKET_CACHE = "bucketCache";
    public static final String EVENT_SCHEDULER = "event_scheduler";

    static {
        System.setProperty("dm.entity.packages.scan", "com.walmartlabs.components.scheduler.model");
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            System.out.println("uncaught exception: " + t);
            e.printStackTrace();
        });
    }

    @Autowired
    private transient DataManager<Long, Bucket> dataManager;

    @Autowired
    private Hz hz;

    private final AtomicReference<Boolean> isShutdown = new AtomicReference<>(false);

    private static final ScheduledExecutorService EXECUTOR_SERVICE =
            new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "EventSchedulerThread"));
    private static final Logger L = Logger.getLogger(ScheduleScanner.class);

    @Override
    public String name() {
        return "EventScanner";
    }

    @Override
    public void init() {
        L.info("initing the event scheduler");
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
        EXECUTOR_SERVICE.scheduleAtFixedRate(this::scan, delay, MILLISECONDS.convert(scanInterval, MINUTES), MILLISECONDS);
        L.info("executing first time scan");
        scan();
    }

    private final Map<Long, BucketStatus> processedBuckets = new ConcurrentHashMap<>();

    private void scan() {
        if (isShutdown.get()) {
            L.info("system is shutdown, no more schedules will be processed");
            return;
        }
        final Integer bucketWidth = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
        final ZonedDateTime now = now(UTC);
        final long currentBucketId = bucketize(now.toInstant().toEpochMilli(), bucketWidth);
        final String bucket = utc(currentBucketId).toString();
        L.debug(format("%s, scanning the schedule(s)", bucket));
        try {
            L.debug(format("%s, starting scan for bucketId: %d, now: %s ", bucket, currentBucketId, now));
            final int lookbackRange = PROPS.getInteger("events.backlog.check.limit", 2);
            final Set<Long> bucketIds = new HashSet<>();
            for (long i = 0; i <= lookbackRange; i++) {
                final long bucketId = currentBucketId - i * bucketWidth * 60 * 1000;
                bucketIds.add(bucketId);
            }
            newHashSet(processedBuckets.keySet()).forEach(bId -> {
                if (!bucketIds.contains(bId))
                    processedBuckets.remove(bId);
                else if (processedBuckets.get(bId) == PROCESSED)
                    bucketIds.remove(bId);
            });
            L.debug(format("%s, buckets to be scheduled: %s ", currentBucketId, bucketIds));
            L.debug(format("%s, calculating scan for buckets: %s", bucket, bucketIds.stream().sorted().map(b -> utc(b).toString()).collect(toList())));
            final ListenableFuture<Multimap<Integer, Pair<Long, Integer>>> dist = calculateTaskDistribution(bucket, bucketIds);
            transformAsync(dist, distribution -> {
                if (distribution.isEmpty()) {
                    L.debug(format("%s, nothing to schedule for bucketIds: %s", bucket, bucketIds));
                    return NO_OP;
                }
                if (!bucketIds.isEmpty())
                    L.debug(format("%s, following bucketIds will not be scheduled, (either no events or all processed): %s", bucket, bucketIds));

                final int submitRetries = PROPS.getInteger("event.submit.max.retries", 10);
                final int submitInitialDelay = PROPS.getInteger("event.submit.initial.delay", 1);
                final int submitBackoff = PROPS.getInteger("event.submit.backoff.multiplier", 1);

                final IExecutorService executorService = hz.hz().getExecutorService(EVENT_SCHEDULER);
                final Iterator<Member> iterator = cycle(hz.hz().getCluster().getMembers());
                final Map<Integer, Collection<Pair<Long, Integer>>> map = distribution.asMap();

                return transformAsync(successfulAsList(map.entrySet().stream().map(e -> taskExecutor.async(() -> () ->
                                transform(ListenableFutureAdapter.adapt(executorService.submitToMember(
                                        new BulkShardTask(e.getValue()), iterator.next())), ShardStatusList::getList),
                        "event-submit", submitRetries, submitInitialDelay, submitBackoff, SECONDS)).collect(toList())), ll -> {
                    final Map<Long, BucketStatus> m = new HashMap<>();
                    newArrayList(concat(ll)).forEach(s -> {
                        final long bucketId = s.getBucketId();
                        if (!m.containsKey(bucketId))
                            m.put(bucketId, s.getStatus());
                        else if (m.get(bucketId) != ERROR) {
                            m.put(bucketId, s.getStatus());
                        }
                    });
                    processedBuckets.putAll(m);
                    return transform(successfulAsList(m.entrySet().stream().map(e -> {
                        final Bucket b = entity(Bucket.class, e.getKey());
                        b.setStatus(e.getValue().name());
                        return transform(dataManager.saveAsync(b), (Function<Bucket, Bucket>) DataManager::raw);
                    }).collect(toList())), (Function<List<Bucket>, List<Bucket>>) l1 -> {
                        L.info(format("%s, buckets processed: " + l1, bucket));
                        return l1;
                    });
                });
            });
        } catch (Exception e) {
            L.error(format("%s, schedule scan failed", bucket), getRootCause(e));
        }
    }

    private final TaskExecutor taskExecutor = new TaskExecutor(newHashSet(Exception.class));
    private static final ListenableFuture<List<Bucket>> NO_OP = immediateFuture(new ArrayList<>());

    private ListenableFuture<Multimap<Integer, Pair<Long, Integer>>> calculateTaskDistribution(String calcId, final Set<Long> bucketIds) {
        L.debug(format("%s, checking if following bucketIds have events: %s", calcId, bucketIds));
        return transform(successfulAsList(bucketIds.stream().map(bId -> transform(dataManager.getAsync(bId, fullSelector(bId)),
                (Function<Bucket, Bucket>) b -> {
                    if (b == null || b.getCount() == 0 || PROCESSED.name().equals(b.getStatus())) {
                        processedBuckets.put(bId, b == null || b.getStatus() == null ? PROCESSED : BucketStatus.valueOf(b.getStatus()));
                    }
                    return b;
                })).collect(toList())),
                (Function<List<Bucket>, Multimap<Integer, Pair<Long, Integer>>>) l -> {
                    final List<Bucket> lb = l.stream().
                            filter(b -> b != null && b.getCount() > 0 && !PROCESSED.name().equals(b.getStatus())).collect(toList());
                    Multimap<Long, Integer> map = create();
                    lb.forEach(b -> {
                        L.debug(format("%s, scheduling bucket: %d", calcId, b.id()));
                        bucketIds.remove(b.id());
                        L.debug(format("%s, calculating schedule distribution for bucketId: %d", calcId, b.id()));
                        int shardSize = PROPS.getInteger("event.shard.size", 1000);
                        final int count = (int) b.getCount();
                        final int numShards = count % shardSize == 0 ? count / shardSize : count / shardSize + 1;
                        L.debug(format("%s, bucketId %d has %d events, and %d distribution", calcId, b.id(), b.getCount(), numShards));
                        for (int i = 0; i < numShards; i++) {
                            map.put(b.id(), i);
                        }
                    });
                    final List<Member> members = newArrayList(hz.hz().getCluster().getMembers());
                    final List<Entry<Long, Integer>> entries = newArrayList(map.entries());
                    final Multimap<Integer, Pair<Long, Integer>> distro = create();
                    final int size = members.size();
                    for (int i = 0; i < entries.size(); i++) {
                        final Entry<Long, Integer> e = entries.get(i);
                        distro.put(i % size, of(e.getKey(), e.getValue()));
                    }
                    L.info(format("%s, schedule distribution: " + distro, calcId));
                    return distro;
                });
    }

    public void shutdown() {
        isShutdown.set(true);
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }
}
