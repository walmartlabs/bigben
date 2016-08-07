package com.walmartlabs.components.scheduler.core;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.hazelcast.core.IMap;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmartlabs.components.scheduler.entities.Bucket;
import com.walmartlabs.components.scheduler.entities.BucketDO;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventDO.EventKey;
import com.walmartlabs.components.scheduler.entities.Status;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmartlabs.components.scheduler.core.ScheduleScanner.SCHEDULER;
import static com.walmartlabs.components.scheduler.entities.Status.*;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.nowUTC;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Created by smalik3 on 6/29/16
 */
public class BucketManager {

    static final Logger L = Logger.getLogger(BucketManager.class);

    private final int maxBuckets;
    private final int maxProcessingTime;
    private final DataManager<ZonedDateTime, Bucket> dataManager;
    private final DataManager<EventKey, Event> eventDataManager;
    private final int lookbackRange;
    private final Map<ZonedDateTime, BucketSnapshot> buckets = new ConcurrentHashMap<>();
    private final int bucketWidth;
    private volatile BucketsLoader bucketsLoader;
    private final StatusSyncer statusSyncer;
    private final CheckpointHelper checkpointHelper;
    private final IMap<ZonedDateTime, Bucket> map;
    private final int shardSize;

    @SuppressWarnings("unchecked")
    public BucketManager(int maxBuckets, int maxProcessingTime, DataManager<?, ?> dm, int bucketWidth,
                         int checkpointInterval, TimeUnit checkpointUnit, int lookbackRange, IMap<ZonedDateTime, Bucket> map) {
        this.maxBuckets = maxBuckets;
        this.maxProcessingTime = maxProcessingTime;
        this.dataManager = (DataManager<ZonedDateTime, Bucket>) dm;
        this.eventDataManager = (DataManager<EventKey, Event>) dm;
        this.lookbackRange = lookbackRange;
        this.bucketWidth = bucketWidth;
        this.shardSize = PROPS.getInteger("event.shard.size", 1000);
        checkpointHelper = new CheckpointHelper(this);
        statusSyncer = new StatusSyncer(dataManager, eventDataManager);
        L.info(format("saving checkpoint every %d %s", checkpointInterval, checkpointUnit));
        SCHEDULER.scheduleAtFixedRate(this::saveCheckpoint, checkpointInterval, checkpointInterval, checkpointUnit);
        try {
            L.info("loading the previously saved checkpoint, if any");
            transform(checkpointHelper.loadCheckpoint(), (Function<Map<ZonedDateTime, BucketSnapshot>, Object>) data -> {
                L.info("loaded previous checkpoint: " + data);
                buckets.putAll(data);
                purgeIfNeeded();
                return null;
            }).get();
        } catch (Exception e) {
            L.error("could not load previous checkpoint", e);
        }
        getRuntime().addShutdownHook(new Thread(() -> {
            L.info("saving checkpoint during shutdown");
            saveCheckpoint();
        }));
        this.map = map;
    }

    private static final Selector<ZonedDateTime, Bucket> selector = fullSelector(nowUTC());

    ListenableFuture<Multimap<ZonedDateTime, Integer>> getProcessableShardsForOrBefore(ZonedDateTime bucketId) {
        if (bucketsLoader == null) {
            L.info("starting the background load of previous buckets");
            final Integer fetchSize = PROPS.getInteger("buckets.background.load.fetch.size", 10);
            bucketsLoader = new BucketsLoader(lookbackRange, fetchSize,
                    b -> buckets.put(b.id(), new BucketSnapshot(b.id(), b.getCount(), shardSize, b.getStatus())),
                    buckets::containsKey, bucketWidth, dataManager, bucketId);
            bucketsLoader.start();
        }

        final ListenableFuture<Bucket> f = dataManager.getAsync(bucketId, selector);//adapt(map.getAsync(bucketId));
        final Multimap<ZonedDateTime, Integer> processableShards = HashMultimap.create();
        addCallback(f, new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(Bucket bucket) {
                bucket = bucket == null ? createEmptyBucket(bucketId) : bucket;
                if (buckets.putIfAbsent(bucketId, new BucketSnapshot(bucketId, bucket.getCount(), shardSize, bucket.getStatus())) != null) {
                    L.warn(format("bucket with id %s already existed in the cache, this is highly unusual", bucketId));
                }
                buckets.entrySet().stream().filter(e -> e.getValue().awaiting().cardinality() > 0).forEach(e ->
                        e.getValue().awaiting().stream().forEach(s -> processableShards.put(e.getKey(), s)));
                L.info(format("processable shards at bucket: %s, are => %s", bucketId, processableShards));
                if (!processableShards.containsKey(bucketId)) {
                    L.info(format("no events in the bucket: %s", bucketId));
                }
            }

            @Override
            public void onFailure(Throwable t) {
                L.info(format("error in loading bucket: %s, will be retried again during next scan", bucketId), t);
            }
        });
        return transform(f, (Function<Bucket, Multimap<ZonedDateTime, Integer>>) $ -> processableShards);
    }

    synchronized void registerForProcessing(Collection<Pair<ZonedDateTime, Integer>> pairs) {
        pairs.forEach(p -> buckets.get(p.getLeft()).processing(p.getRight()));
        purgeIfNeeded();
        startShardsTimer(pairs);
    }

    private ListenableScheduledFuture<?> startShardsTimer(Collection<Pair<ZonedDateTime, Integer>> pairs) {
        final List<String> shards = pairs.stream().sorted().map(p -> p.getLeft() + "[" + p.getRight() + "]").collect(toList());
        L.info(format("starting processing timer for shards: %s", shards));
        return SCHEDULER.schedule(() -> checkShardsStatus(pairs, shards), maxProcessingTime, SECONDS);
    }

    private synchronized void checkShardsStatus(Collection<Pair<ZonedDateTime, Integer>> pairs, List<String> shards) {
        try {
            for (Pair<ZonedDateTime, Integer> pair : pairs) {
                final ZonedDateTime bucketId = pair.getLeft();
                final int shard = pair.getRight();
                final BucketSnapshot bd = buckets.get(bucketId);
                if (bd != null && bd.processing().get(shard)) {
                    L.warn(format("bulk timer for shard: %s[%d] expired, marking the shard as failure", bucketId, shard));
                    bd.done(shard, ERROR);
                }
            }
        } catch (Exception e) {
            L.error(format("error in timing out the shards for processing, shards: %s", shards), e);
        }
    }

    synchronized void shardDone(ZonedDateTime bucketId, Integer shard, Status status) {
        if (!buckets.containsKey(bucketId)) {
            L.warn(format("bucket %s not found in cache, might have been purged, ignoring this call", bucketId));
            return;
        }
        final BucketSnapshot bd = buckets.get(bucketId);
        bd.done(shard, status);
        if (bd.processing().cardinality() == 0) {
            final Status bucketStatus = bd.awaiting().cardinality() == 0 ? PROCESSED : ERROR;
            L.info(format("all shards done for bucket: %s, marking it %s", bucketId, bucketStatus));
            bucketProcessed(bucketId, bucketStatus);
        }
    }

    private static final ListenableFuture<Bucket> NO_OP = immediateFuture(null);

    synchronized ListenableFuture<Bucket> bucketProcessed(ZonedDateTime bucketId, Status status) {
        final BucketSnapshot bd = buckets.get(bucketId);
        if (bd == null || bd.awaiting().cardinality() == 0)
            return NO_OP;
        L.info(format("marking bucket %s as " + status, bucketId));
        bd.processing().clear();
        if (status == PROCESSED)
            bd.awaiting().clear();
        return statusSyncer.syncBucket(bucketId, status, true);
    }

    private synchronized void purgeIfNeeded() {
        try {
            final List<ZonedDateTime> bucketIds = reverse(newArrayList(new TreeSet<>(buckets.keySet())));
            final List<BucketSnapshot> purged = new ArrayList<>();
            L.debug("checking buckets for purging from shards: " + buckets);
            if (bucketIds.size() > maxBuckets) {
                for (int i = maxBuckets; i < bucketIds.size(); i++) {
                    final ZonedDateTime bucketId = bucketIds.get(i);
                    final BucketSnapshot bs = buckets.get(bucketId);
                    if (bs.processing().cardinality() > 0)
                        L.info(format("bucket %s is currently being processed, cancelling purge for this bucket", bucketId));
                    else buckets.remove(bucketId);
                    L.debug("purging bucket snapshot: " + bs);
                    purged.add(bs);
                    if (bs.count() == 0)
                        continue;
                    if (bs.processing().cardinality() == 0 && bs.awaiting().cardinality() == 0) {
                        statusSyncer.syncBucket(bucketId, PROCESSED, false);
                    } else {
                        statusSyncer.syncBucket(bucketId, ERROR, false);
                    }
                    purged.add(bs);
                }
            }
            if (!purged.isEmpty()) {
                L.info("purged snapshots: " + purged);
            } else L.info("nothing to purge");

        } catch (Exception e) {
            L.error("error in purging snapshots", e);
        }
    }

    synchronized void saveCheckpoint() {
        purgeIfNeeded();
        checkpointHelper.saveCheckpoint(buckets);
    }

    int getMaxProcessingTime() {
        return maxProcessingTime;
    }

    DataManager<EventKey, Event> getEventDataManager() {
        return eventDataManager;
    }

    StatusSyncer getStatusSyncer() {
        return statusSyncer;
    }

    static Bucket createEmptyBucket(ZonedDateTime bId) {
        final BucketDO bucketDO = new BucketDO();
        bucketDO.setId(bId);
        bucketDO.setStatus(EMPTY.name());
        bucketDO.setCount(0);
        return bucketDO;
    }
}
