package com.walmartlabs.components.scheduler.core;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.model.Bucket;
import com.walmartlabs.components.scheduler.model.Bucket.BucketStatus;
import com.walmartlabs.components.scheduler.model.Event;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;
import org.apache.log4j.Logger;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.beust.jcommander.internal.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmart.services.common.util.JsonUtil.convertToObject;
import static com.walmart.services.common.util.JsonUtil.convertToString;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.*;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by smalik3 on 6/29/16
 */
public class BucketManager {

    private static final Logger L = Logger.getLogger(BucketManager.class);

    private final int maxBuckets;
    private final int maxProcessingTime;
    private final ListeningScheduledExecutorService service;
    private final DataManager<Long, Bucket> dataManager;
    private final DataManager<EventKey, Event> dm;
    private final AtomicBoolean modified = new AtomicBoolean(false);
    private final Table<Long, Integer, BucketStatus> shards = HashBasedTable.create();
    private final int bucketWidth;

    @SuppressWarnings("unchecked")
    public BucketManager(int maxBuckets, int maxProcessingTime, DataManager<?, ?> dm) {
        this.maxBuckets = maxBuckets;
        this.maxProcessingTime = maxProcessingTime;
        this.dataManager = (DataManager<Long, Bucket>) dm;
        this.dm = (DataManager<EventKey, Event>) dm;
        service = listeningDecorator(newScheduledThreadPool(1));
        final Integer checkpointInterval = PROPS.getInteger("event.bucket.manager.checkpoint.interval", 1);
        this.bucketWidth = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
        service.scheduleAtFixedRate(this::saveCheckpoint, checkpointInterval, checkpointInterval, MINUTES);
        try {
            L.info("loading the previously saved checkpoint, if any");
            loadCheckpoint().get();
        } catch (Exception e) {
            L.error("could not load previous checkpoint", e);
        }
    }

    public ListenableFuture<Multimap<Long, Integer>> getProcessableShardsForOrBefore(long bucketId, int lookbackRange) {
        final Multimap<Long, Integer> processableShards = HashMultimap.create();
        final List<ListenableFuture<Bucket>> futures = new ArrayList<>();
        for (long i = 0; i <= lookbackRange; i++) {
            final long bId = bucketId - i * bucketWidth * 60 * 1000;
            if (!shards.containsRow(bId)) {
                futures.add(dataManager.getAsync(bId, fullSelector(bId)));
            } else {
                shards.row(bId).forEach((s, b) -> {
                    if (b != PROCESSING && b != PROCESSED)
                        processableShards.put(bId, s);
                });
            }
        }
        return transform(successfulAsList(futures), (com.google.common.base.Function<List<Bucket>, Multimap<Long, Integer>>) l -> {
            l.forEach(b -> {
                if (b != null && b.getCount() > 0 && !PROCESSED.name().equals(b.getStatus())) {
                    int shardSize = PROPS.getInteger("event.shard.size", 1000);
                    final int count = (int) b.getCount();
                    final int numShards = count % shardSize == 0 ? count / shardSize : count / shardSize + 1;
                    for (int i = 0; i < numShards; i++) {
                        processableShards.put(b.id(), i);
                    }
                }
            });
            L.info(format("processable shards at bucket: %d, are => %s", bucketId, processableShards));
            return processableShards;
        });
    }

    public ListenableFuture<Event> saveCheckpoint() {
        try {
            L.info("saving checkpoint for shards table");
            return syncShard(-1, -1, -1, "", PROCESSING, convertToString(shards.rowMap()));
        } catch (IOException e) {
            return immediateFailedFuture(e);
        }
    }

    private ListenableFuture<?> loadCheckpoint() {
        final EventKey key = EventKey.of(-1, -1, -1, "");
        final ListenableFuture<Event> f = dm.getAsync(key, fullSelector(key));
        addCallback(f,
                new FutureCallback<Event>() {
                    @Override
                    public void onSuccess(Event result) {
                        if (result == null)
                            L.info("no checkpoint to load");
                        else if (result.getPayload() != null) {
                            try {
                                final Map<Long, Map<Integer, BucketStatus>> map = convertToObject(result.getPayload(),
                                        new TypeReference<Map<Long, Map<Integer, BucketStatus>>>() {
                                        });
                                map.forEach((k, v) -> v.forEach((s, b) -> shards.put(k, s, b)));
                                L.info("loaded the checkpoint: " + shards);
                            } catch (IOException e) {
                                L.error("could not load the previously saved checkpoint: " + result.getPayload(), e);
                            }
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        L.error("could not load the previously saved bucket status data", t);
                    }
                });
        return f;
    }

    /*public Multimap<Long, Integer> pendingShards() {
        shards.cellSet().stream().filter(c -> c.getValue() == UN_PROCESSED || c.getValue() == ERROR).
                collect(groupingBy(new Function<Cell<Long, Integer, BucketStatus>, Long>() {
                    @Override
                    public Long apply(Cell<Long, Integer, BucketStatus> longIntegerBucketStatusCell) {
                        return null;
                    }
                }, ));
    }*/

    public void registerForProcessing(Long bucketId, Integer shard) {
        modified.set(true);
        purgeShardsIfNeeded();
        shards.put(bucketId, shard, PROCESSING);
        service.schedule(() -> {
            final BucketStatus bucketStatus = this.shards.get(bucketId, shard);
            if (bucketStatus != null && bucketStatus == PROCESSING) {
                shards.put(bucketId, shard, UN_PROCESSED);
                syncShard(bucketId, shard, -1, "", TIMED_OUT, null);
            }
        }, maxProcessingTime, SECONDS);
    }

    public void shardDone(long bucketId, int shard, BucketStatus shardStatus) {
        modified.set(true);
        switch (shardStatus) {
            case PROCESSED:
                shards.remove(bucketId, shard);
                if (!shards.containsRow(bucketId)) {
                    syncBucket(bucketId, PROCESSED);
                }
                break;
            case ERROR:
                shards.put(bucketId, shard, ERROR);
                break;
            default:
                throw new IllegalArgumentException(format("bucket: %d, shard: %d, status: %s", bucketId, shard, shardStatus.name()));
        }
    }

    private void purgeShardsIfNeeded() {
        final List<Long> bucketIds = reverse(newArrayList(new TreeSet<>(shards.rowKeySet())));
        if (bucketIds.size() > maxBuckets) {
            for (int i = maxBuckets; i < bucketIds.size(); i++) {
                final Long bucketId = bucketIds.get(i);
                final Map<Integer, BucketStatus> statusByShard = shards.row(bucketId);
                L.info(format("purging the following shards for bucket %d, => %s", bucketId, statusByShard));
                final Set<BucketStatus> statuses = new HashSet<>(statusByShard.values());
                if (statuses.contains(ERROR)) {
                    syncBucket(bucketId, ERROR);
                } else if (statuses.contains(UN_PROCESSED) || statuses.contains(PROCESSING)) {
                    syncBucket(bucketId, UN_PROCESSED);
                } else if (statuses.size() == 1 && statuses.contains(PROCESSED)) {
                    syncBucket(bucketId, PROCESSED);
                }
                for (Integer shard : statusByShard.keySet()) {
                    shards.remove(bucketId, shard);
                }
            }
        }
    }

    private ListenableFuture<Bucket> syncBucket(final long bucketId, BucketStatus bucketStatus) {
        final Bucket entity = entity(Bucket.class, bucketId);
        entity.setProcessedAt(new Date());
        entity.setStatus(bucketStatus.name());
        L.info(format("bucket %d is done, syncing status as %s", bucketId, bucketStatus));
        final ListenableFuture<Bucket> f = dataManager.saveAsync(entity);
        addCallback(f,
                new FutureCallback<Bucket>() {
                    @Override
                    public void onSuccess(Bucket result) {
                        L.info(format("bucket %d is successfully synced as %s", bucketId, bucketStatus));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        L.error(format("bucket %d could not be synced as %s, after multiple retries", bucketId, bucketStatus), t);
                    }
                });
        return f;
    }

    private ListenableFuture<Event> syncShard(long bucketId, int shard, long eventTime, String eventId, BucketStatus bucketStatus, String payload) {
        final Event entity = entity(Event.class, EventKey.of(bucketId, shard, eventTime, eventId));
        entity.setStatus(TIMED_OUT.name());
        if (payload != null)
            entity.setPayload(payload);
        L.info(format("shard %d[%d] is done, syncing status as %s", bucketId, shard, bucketStatus));
        final ListenableFuture<Event> f = dm.saveAsync(entity);
        addCallback(f,
                new FutureCallback<Event>() {
                    @Override
                    public void onSuccess(Event result) {
                        L.info(format("shard %d[%d] is successfully synced as %s", bucketId, shard, bucketStatus));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        L.error(format("shard %d[%d] could not be synced as %s, after multiple retries", bucketId, shard, bucketStatus), t);
                    }
                });
        return f;
    }
}
