package com.walmartlabs.components.scheduler.core.hz;

import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.query.Predicate;
import com.walmart.gmp.ingestion.platform.framework.core.Hz;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.services.Service;
import com.walmartlabs.components.scheduler.core.ScheduleJob;
import com.walmartlabs.components.scheduler.model.EventBucketStatusEntity;
import com.walmartlabs.components.scheduler.utils.TimeUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Lists.partition;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.newHashSet;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmartlabs.components.scheduler.model.EventBucketStatusEntity.BucketStatus.PROCESSED;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.toAbsolute;
import static java.lang.String.format;
import static java.time.LocalDateTime.now;
import static java.time.LocalDateTime.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Created by smalik3 on 3/8/16
 */
public class HzEventScheduleScanner implements Service {

    static final String BUCKET_CACHE = "bucketCache";

    @Autowired
    private transient DataManager<Long, EventBucketStatusEntity> dataManager;

    @Autowired
    private Hz hz;

    private static final ScheduledExecutorService EXECUTOR_SERVICE =
            new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "EventSchedulerThread"));
    private static final Logger L = Logger.getLogger(HzEventScheduleScanner.class);

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
        L.info("calculating the next scan offset");
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
            final long currentOffset = chronoUnit.between(of(now().getYear(), 1, 1, 0, 0), now());
            final int pastOffsets = PROPS.getInteger("events.backlog.check.limit", 2);
            final IMap<Long, EventBucketStatusEntity> cache = hz.hz().getMap(BUCKET_CACHE);
            Set<Long> offsets = new HashSet<>();
            for (long i = currentOffset - pastOffsets; i <= currentOffset; i++) {
                offsets.add(i);
            }
            L.debug(format("%d, checking if following offsets have schedules: %s", currentOffset, offsets));
            cache.getAll(offsets);
            final AtomicBoolean atLeastOne = new AtomicBoolean();
            transformValues(cache.executeOnEntries(null,
                    new Predicate<Long, EventBucketStatusEntity>() {
                        @Override
                        public boolean apply(Entry<Long, EventBucketStatusEntity> e) {
                            return offsets.contains(e.getKey()) && e.getValue() != null && e.getValue().getCount() > 0
                                    && !PROCESSED.name().equals(e.getValue().getStatus());
                        }
                    }), $ -> (EventBucketStatusEntity) $).forEach((k, v) -> {

                L.debug(format("%d, scheduling offset: %d", currentOffset, k));
                atLeastOne.set(true);
                offsets.remove(k);
                L.debug(format("%d, calculating schedule distribution for offset: %d", currentOffset, k));
                int shardSize = PROPS.getInteger("event.shard.size", 1000);
                final int numShards = (int) v.getCount() / shardSize + 1;
                L.debug(format("%d, offset: %d, numShards: %d, count: %d, shardSize: %d", currentOffset, k, numShards, v.getCount(), shardSize));
                L.debug(format("%d, submitting the schedules over the grid", currentOffset));
                final List<Integer> shardIndexes = new ArrayList<>();
                for (long i = 0; i < numShards; i++) {
                    shardIndexes.add((int) i);
                }
                final int gridSize = hz.hz().getCluster().getMembers().size();
                final List<List<Integer>> partitions = partition(shardIndexes, numShards < gridSize ? 1 : numShards / gridSize);
                final long bucketKey = toAbsolute(k);
                L.debug(format("%d, distribution profile: bucketKey: %d, nodes: %d, partitions: %s", currentOffset, bucketKey, gridSize, partitions));
                cache.executeOnKey(k,
                        new AbstractEntryProcessor<Long, EventBucketStatusEntity>() {
                            @Override
                            public Object process(Entry<Long, EventBucketStatusEntity> entry) {
                                entry.getValue().setJobCount(partitions.size());
                                entry.setValue(entry.getValue());
                                return null;
                            }
                        });
                //TODO: create an executor and may be listen for job failure:
                partitions.forEach(p -> hz.hz().getExecutorService("executor").submit(new ScheduleJob(bucketKey, newHashSet(p), k)));
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
