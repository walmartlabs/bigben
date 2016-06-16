package com.walmartlabs.components.scheduler.core.hz;

import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.query.Predicate;
import com.walmart.gmp.ingestion.platform.framework.core.Hz;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmartlabs.components.scheduler.model.Bucket;
import com.walmartlabs.components.scheduler.services.Service;
import com.walmartlabs.components.scheduler.utils.TimeUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.partition;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.walmart.gmp.ingestion.platform.framework.core.ListenableFutureAdapter.adapt;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.PROCESSED;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.toAbsolute;
import static java.lang.String.format;
import static java.time.LocalDateTime.now;
import static java.time.LocalDateTime.of;
import static java.util.concurrent.TimeUnit.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Created by smalik3 on 3/8/16
 */
public class ScheduleScanner implements Service {

    static final String BUCKET_CACHE = "bucketCache";

    @Autowired
    private transient DataManager<Long, Bucket> dataManager;

    @Autowired
    private Hz hz;

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
        if (true)
            return;
        L.info("executing the EventScheduleScanner");
        final Integer scanInterval = PROPS.getInteger("event.schedule.scan.interval.minutes", 60);
        L.info("calculating the next scan bucketId");
        final LocalDateTime now = now();
        final LocalDateTime nextScan = TimeUtils.nextScan(now, scanInterval);
        final long delay = ChronoUnit.MILLIS.between(now, nextScan);
        L.info(format("first-scan at: %s, next-scan at: %s, " +
                "initial-delay: %d ms, subsequent-scans: after every %d minutes", now(), nextScan, delay, scanInterval));
        EXECUTOR_SERVICE.scheduleAtFixedRate(this::scan, delay, MILLISECONDS.convert(scanInterval, MINUTES), MILLISECONDS);
        L.info("executing first time scan");
        scan();
    }

    private void scan() {
        L.debug("scanning the schedule(s)");
        try {
            final ChronoUnit chronoUnit = ChronoUnit.valueOf(PROPS.getProperty("event.schedule.scan.unit", ChronoUnit.HOURS.name()));
            final long currentBucketId = chronoUnit.between(of(now().getYear(), 1, 1, 0, 0), now());
            final int lookbackRange = PROPS.getInteger("events.backlog.check.limit", 2);
            final Set<Long> bucketIds = new HashSet<>();
            for (long i = currentBucketId - lookbackRange; i <= currentBucketId; i++) {
                bucketIds.add(i);
            }
            final Map<Long, Set<Integer>> shards = calculateScheduleDistribution(currentBucketId, bucketIds);

            if (shards.isEmpty()) {
                L.debug(format("%d, nothing to schedule for bucketIds: %s", currentBucketId, bucketIds));
                return;
            }
            if (!bucketIds.isEmpty())
                L.debug(format("%d, following bucketIds will not be scheduled, (either no events or all processed): %s", currentBucketId, bucketIds));

            L.debug("submitting the schedules for execution" + shards);
            final IExecutorService executorService = hz.hz().getExecutorService("default");
            final Set<Member> members = hz.hz().getCluster().getMembers();
            final Iterator<Member> iterator = members.iterator();
            transformAsync(successfulAsList(partition(newArrayList(shards.entrySet()), members.size() != 1 ? shards.size() / members.size() : 1).stream().
                    map(l -> l.stream().collect(toMap(Entry::getKey, Entry::getValue))).
                    collect(toList()).stream().map(BulkEventTask::new).collect(toList()).stream().map(e ->
                    submit(executorService, e, iterator.next())).collect(toList())), l -> successfulAsList(newArrayList(concat(
                    l.stream().map(m -> m.entrySet().stream().map(e -> {
                        final Bucket entity = entity(Bucket.class, e.getKey());
                        entity.setFailedEventsId(e.getValue());
                        return dataManager.saveAsync(entity);
                    }).collect(toList())).collect(toList())))));
        } catch (Exception e) {
            L.error("schedule scan failed", getRootCause(e));
        }
    }

    private final TaskExecutor taskExecutor = new TaskExecutor(newHashSet(Exception.class));

    private ListenableFuture<Map<Long, String>> submit(IExecutorService executorService, BulkEventTask task, Member member) {
        final ListenableFuture<Map<Long, String>> f = adapt(executorService.submitToMember(task, member));
        return taskExecutor.async(() -> f, "",
                PROPS.getInteger("event.submit.max.retries", 3),
                PROPS.getInteger("event.submit.retry.initial.delay", 1),
                PROPS.getInteger("event.submit.back.off.multiplier", 2), SECONDS);
    }

    private Map<Long, Set<Integer>> calculateScheduleDistribution(long calcId, final Set<Long> bucketIds) {
        final IMap<Long, Bucket> cache = hz.hz().getMap(BUCKET_CACHE);
        L.debug(format("%d, checking if following bucketIds have schedules: %s", calcId, bucketIds));
        cache.getAll(bucketIds);
        return transformValues(cache.executeOnEntries(null,
                new Predicate<Long, Bucket>() {
                    @Override
                    public boolean apply(Entry<Long, Bucket> e) {
                        return bucketIds.contains(e.getKey()) && e.getValue() != null && e.getValue().getCount() > 0
                                && !PROCESSED.name().equals(e.getValue().getStatus());
                    }
                }), $ -> (Bucket) $).entrySet().stream().collect(toMap(Entry::getKey, e -> {
            final Long bucketId = e.getKey();
            final Bucket bucket = e.getValue();
            L.debug(format("%d, scheduling bucket: %d", calcId, bucketId));
            bucketIds.remove(bucketId);
            L.debug(format("%d, calculating schedule distribution for bucketId: %d", calcId, bucketId));
            int shardSize = PROPS.getInteger("event.shard.size", 1000);
            final int numShards = (int) bucket.getCount() / shardSize + 1;
            L.debug(format("%d, bucketId: %d, numShards: %d, count: %d, shardSize: %d", calcId, bucketId, numShards, bucket.getCount(), shardSize));
            L.debug(format("%d, submitting the schedules over the grid", calcId));
            final List<Integer> shardIndexes = new ArrayList<>();
            for (int i = 0; i < numShards; i++) {
                shardIndexes.add(i);
            }
            final int gridSize = hz.hz().getCluster().getMembers().size();
            final List<List<Integer>> partitions = partition(shardIndexes, numShards < gridSize ? 1 : numShards / gridSize);
            final long bucketKey = toAbsolute(bucketId);
            L.debug(format("%d, distribution profile: bucketKey: %d, nodes: %d, partitions: %s", calcId, bucketKey, gridSize, partitions));
            return newHashSet(shardIndexes);
        }));
    }
}
