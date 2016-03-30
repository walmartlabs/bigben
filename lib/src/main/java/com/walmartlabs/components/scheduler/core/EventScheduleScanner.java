package com.walmartlabs.components.scheduler.core;

import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.TimeUtils;
import com.walmartlabs.components.scheduler.model.EventBucketStatusEntity;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.log4j.Logger;

import javax.cache.processor.EntryProcessor;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Lists.partition;
import static com.google.common.collect.Sets.newHashSet;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmartlabs.components.scheduler.TimeUtils.toAbsolute;
import static com.walmartlabs.components.scheduler.model.EventBucketStatusEntity.BucketStatus.PROCESSED;
import static java.lang.String.format;
import static java.time.LocalDateTime.now;
import static java.time.LocalDateTime.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Created by smalik3 on 3/8/16
 */
public class EventScheduleScanner implements Service, Runnable {

    @IgniteInstanceResource
    private transient Ignite ignite;

    static final String BUCKET_CACHE = "bucketCache";

    @SpringResource(resourceName = "dataManager")
    private transient DataManager<Long, EventBucketStatusEntity> dataManager;

    private static final ScheduledExecutorService EXECUTOR_SERVICE =
            new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "EventSchedulerThread"));
    private static final Logger L = Logger.getLogger(EventScheduleScanner.class);

    @Override
    public void cancel(ServiceContext ctx) {
        L.info("stopping the EventScheduleScanner");
        EXECUTOR_SERVICE.shutdown();
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        L.info("starting the EventScheduleScanner");
        //L.info("bootstrapping the bucket cache");
        //ignite.cache(BUCKET_CACHE).loadCache(null);
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        if (ctx.isCancelled()) {
            L.warn("service context has been cancelled. No more tasks will be scheduled");
            return;
        }
        L.info("executing the EventScheduleScanner");
        final Integer scanInterval = PROPS.getInteger("event.schedule.scan.interval.minutes", 60);
        L.info("calculating the next scan offset");
        final LocalDateTime now = now();
        final LocalDateTime nextScan = TimeUtils.nextScan(now, scanInterval);
        final long delay = ChronoUnit.MILLIS.between(now, nextScan);
        L.info(format("first-scan at: %s, next-scan at: %s, " +
                "initial-delay: %d ms, subsequent-scans: after every %d minutes", now(), nextScan, delay, scanInterval));
        EXECUTOR_SERVICE.scheduleAtFixedRate(this, delay, MILLISECONDS.convert(scanInterval, MINUTES), MILLISECONDS);
        L.info("executing first time scan");
        run();
    }

    @Override
    public void run() {
        L.debug("scanning the schedule(s)");
        try {
            final ChronoUnit chronoUnit = ChronoUnit.valueOf(PROPS.getProperty("event.schedule.scan.unit", ChronoUnit.HOURS.name()));
            final long currentOffset = chronoUnit.between(of(now().getYear(), 1, 1, 0, 0), now());
            final int pastOffsets = PROPS.getInteger("events.backlog.check.limit", 2);
            final IgniteCache<Long, EventBucketStatusEntity> cache = ignite.cache(BUCKET_CACHE);
            Set<Long> offsets = new HashSet<>();
            for (long i = currentOffset - pastOffsets; i <= currentOffset; i++) {
                offsets.add(i);
            }
            L.debug(format("%d, checking if following offsets have schedules: %s", currentOffset, offsets));
            cache.getAll(offsets);
            final AtomicBoolean atLeastOne = new AtomicBoolean();
            cache.query(new ScanQuery<Long, EventBucketStatusEntity>((k, v) ->
                    offsets.contains(k) && v != null && v.getCount() > 0 && !PROCESSED.name().equals(v.getStatus()))).forEach(e -> {
                L.debug(format("%d, scheduling offset: %d", currentOffset, e.getKey()));
                atLeastOne.set(true);
                offsets.remove(e.getKey());
                L.debug(format("%d, calculating schedule distribution for offset: %d", currentOffset, e.getKey()));
                int shardSize = PROPS.getInteger("event.shard.size", 1000);
                final int shards = (int) e.getValue().getCount() / shardSize + 1;
                L.debug(format("%d, offset: %d, shards: %d, count: %d, shardSize: %d", currentOffset, e.getKey(), shards, e.getValue().getCount(), shardSize));
                L.debug(format("%d, submitting the schedules over the grid", currentOffset));
                final IgniteCompute async = ignite.compute().withAsync();
                final List<Integer> shardIndexes = new ArrayList<>();
                for (long i = 0; i < shards; i++) {
                    shardIndexes.add((int) i);
                }
                final int gridSize = ignite.cluster().nodes().size();
                final List<List<Integer>> partitions = partition(shardIndexes, shards / gridSize);
                final long bucketKey = toAbsolute(e.getKey());
                L.debug(format("%d, distribution profile: bucketKey: %d, nodes: %d, partitions: %s", currentOffset, bucketKey, gridSize, partitions));
                cache.invoke(e.getKey(), (EntryProcessor<Long, EventBucketStatusEntity, Object>) (entry, arguments) -> {
                    entry.getValue().setJobCount(partitions.size());
                    entry.setValue(entry.getValue());
                    return null;
                });
                partitions.forEach(p -> async.run(new ScheduleJob(bucketKey, newHashSet(p), e.getKey())));
            });
            if (!atLeastOne.get())
                L.debug(format("%d, nothing to schedule for offsets: %s", currentOffset, offsets));
            else if (!offsets.isEmpty())
                L.debug(format("%d, following offsets were not scheduled, (either no events or all processed): %s", currentOffset, offsets));
        } catch (Exception e) {
            L.error("schedule scan failed", getRootCause(e));
        }
    }
}
