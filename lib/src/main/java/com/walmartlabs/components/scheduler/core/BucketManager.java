package com.walmartlabs.components.scheduler.core;

import com.google.common.collect.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.model.Bucket;
import com.walmartlabs.components.scheduler.model.Bucket.BucketStatus;
import com.walmartlabs.components.scheduler.model.Event;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.beust.jcommander.internal.Lists.newArrayList;
import static com.google.common.base.Splitter.on;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.*;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.epoch;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.now;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.tuple.Pair.of;

/**
 * Created by smalik3 on 6/29/16
 */
public class BucketManager {

    private static final Logger L = Logger.getLogger(BucketManager.class);

    private final int maxBuckets;
    private final int maxProcessingTime;
    private final ListeningScheduledExecutorService service;
    private final DataManager<ZonedDateTime, Bucket> dataManager;
    private final DataManager<EventKey, Event> dm;
    private final AtomicBoolean modified = new AtomicBoolean(false);
    private final Table<ZonedDateTime, Integer, BucketStatus> shards = HashBasedTable.create();
    private final int bucketWidth;

    @SuppressWarnings("unchecked")
    public BucketManager(int maxBuckets, int maxProcessingTime, DataManager<?, ?> dm, int bucketWidth, int checkpointInterval, TimeUnit checkpointUnit) {
        this.maxBuckets = maxBuckets;
        this.maxProcessingTime = maxProcessingTime;
        this.dataManager = (DataManager<ZonedDateTime, Bucket>) dm;
        this.dm = (DataManager<EventKey, Event>) dm;
        service = listeningDecorator(newScheduledThreadPool(1));
        this.bucketWidth = bucketWidth;
        L.info(format("scheduling save checkpoint task every %d %s", checkpointInterval, checkpointInterval));
        service.scheduleAtFixedRate(this::saveCheckpoint, checkpointInterval, checkpointInterval, checkpointUnit);
        try {
            L.info("loading the previously saved checkpoint, if any");
            loadCheckpoint().get();
            truncateIfNeeded();
        } catch (Exception e) {
            L.error("could not load previous checkpoint", e);
        }
        getRuntime().addShutdownHook(new Thread(() -> {
            L.info("saving checkpoint during shutdown");
            saveCheckpoint();
        }));
    }

    public synchronized ListenableFuture<Multimap<ZonedDateTime, Integer>> getProcessableShardsForOrBefore(ZonedDateTime bucketId, int lookbackRange) {
        final Multimap<ZonedDateTime, Integer> processableShards = HashMultimap.create();
        final List<ListenableFuture<Bucket>> futures = new ArrayList<>();
        for (long i = 0; i <= lookbackRange; i++) {
            final ZonedDateTime bId = bucketId.minusSeconds(i * bucketWidth);
            if (!shards.containsRow(bId)) {
                futures.add(dataManager.getAsync(bId, fullSelector(bId)));
            } else {
                shards.row(bId).forEach((s, b) -> {
                    if (b != PROCESSING && b != PROCESSED)
                        processableShards.put(bId, s);
                });
            }
        }
        return catching(transform(successfulAsList(futures), (com.google.common.base.Function<List<Bucket>, Multimap<ZonedDateTime, Integer>>) l -> {
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
            L.info(format("processable shards at bucket: %s, are => %s", bucketId, processableShards));
            return processableShards;
        }), Exception.class, ex -> {
            L.error("unexpected error in getting shards", ex);
            return HashMultimap.create();
        });
    }

    public synchronized void registerForProcessing(ZonedDateTime bucketId, Integer shard) {
        modified.set(true);
        truncateIfNeeded();
        shards.put(bucketId, shard, PROCESSING);
        service.schedule(() -> {
            try {
                final BucketStatus bucketStatus = this.shards.get(bucketId, shard);
                if (bucketStatus != null && bucketStatus == PROCESSING) {
                    shards.put(bucketId, shard, UN_PROCESSED);
                    syncShard(bucketId, shard, epoch(), "", UN_PROCESSED, null);
                }
            } catch (Exception e) {
                L.error(format("error in timing out the shard for processing, bucket: %s, shard: %d", bucketId, shard));
            }
        }, maxProcessingTime, SECONDS);
    }

    public synchronized void shardDone(ZonedDateTime bucketId, Integer shard, BucketStatus shardStatus) {
        modified.set(true);
        switch (shardStatus) {
            case PROCESSED:
                L.info(format("shard: %s[%s] finished successfully", bucketId, shard));
                shards.remove(bucketId, shard);
                if (!shards.containsRow(bucketId)) {
                    L.info(format("no more shards to be processed for bucket: %s, marking it PROCESSED", bucketId));
                    syncBucket(bucketId, PROCESSED);
                    shards.put(bucketId, -1, PROCESSED);
                }
                break;
            case ERROR:
                L.info(format("shard: %s[%s] finished with error", bucketId, shard));
                shards.put(bucketId, shard, ERROR);
                break;
            default:
                throw new IllegalArgumentException(format("bucket: %s, shard: %d, status: %s", bucketId, shard, shardStatus.name()));
        }
    }

    public synchronized void bucketDone(ZonedDateTime bucketId, BucketStatus bucketStatus) {
        shards.rowKeySet().stream().filter(z -> z.equals(bucketId)).forEach(z -> {
            final Map<Integer, BucketStatus> row = new HashMap<>(shards.row(z));
            for (Integer shard : row.keySet()) {
                shards.remove(z, shard);
            }
        });
        syncBucket(bucketId, bucketStatus);
    }

    private void truncateIfNeeded() {
        final List<ZonedDateTime> bucketIds = reverse(newArrayList(new TreeSet<>(shards.rowKeySet())));
        final List<Pair<ZonedDateTime, Integer>> purged = new ArrayList<>();
        L.info("checking buckets for purging");
        if (bucketIds.size() > maxBuckets) {
            for (int i = maxBuckets; i < bucketIds.size(); i++) {
                final ZonedDateTime bucketId = bucketIds.get(i);
                final Map<Integer, BucketStatus> statusByShard = shards.row(bucketId);
                L.info(format("purging the following shards for bucket %s, => %s", bucketId, statusByShard));
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
                    purged.add(of(bucketId, shard));
                }
            }
        }
        if (!purged.isEmpty())
            L.info("purged shards: " + purged);
        else L.info("nothing to purge");
    }

    private ListenableFuture<Bucket> syncBucket(ZonedDateTime bucketId, BucketStatus bucketStatus) {
        final Bucket entity = entity(Bucket.class, bucketId);
        entity.setProcessedAt(now(UTC));
        entity.setStatus(bucketStatus.name());
        L.info(format("bucket %s is done, syncing status as %s", bucketId, bucketStatus));
        final ListenableFuture<Bucket> f = dataManager.saveAsync(entity);
        addCallback(f,
                new FutureCallback<Bucket>() {
                    @Override
                    public void onSuccess(Bucket result) {
                        L.info(format("bucket %s is successfully synced as %s", bucketId, bucketStatus));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        L.error(format("bucket %s could not be synced as %s, after multiple retries", bucketId, bucketStatus), t);
                    }
                });
        return f;
    }

    private ListenableFuture<Event> syncShard(ZonedDateTime bucketId, int shard, ZonedDateTime eventTime, String eventId, BucketStatus bucketStatus, String payload) {
        final Event entity = entity(Event.class, EventKey.of(bucketId, shard, eventTime, eventId));
        entity.setStatus(bucketStatus.name());
        if (payload != null)
            entity.setPayload(payload);
        L.info(format("shard %s[%d] is done, syncing status as %s", bucketId, shard, bucketStatus));
        final ListenableFuture<Event> f = dm.saveAsync(entity);
        addCallback(f,
                new FutureCallback<Event>() {
                    @Override
                    public void onSuccess(Event result) {
                        L.info(format("shard %s[%d] is successfully synced as %s", bucketId, shard, bucketStatus));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        L.error(format("shard %s[%d] could not be synced as %s, after multiple retries", bucketId, shard, bucketStatus), t);
                    }
                });
        return f;
    }

    private static final ListenableFuture<Event> SUCCESS = immediateFuture(null);

    public synchronized ListenableFuture<Event> saveCheckpoint() {
        try {
            L.info("saving checkpoint for shards: " + shards);
            return syncShard(epoch(), -1, epoch(), "", PROCESSING, shards.cellSet().stream().sorted((o1, o2) -> {
                final int i = o1.getRowKey().compareTo(o2.getRowKey());
                return i != 0 ? i : o1.getColumnKey().compareTo(o2.getColumnKey());
            }).map(c -> format("%s/%s/%s", c.getRowKey(), c.getColumnKey(), c.getValue())).collect(joining(",")));
        } catch (Exception e) {
            L.error("error in saving the checkpoint: shards: " + shards, e);
            return SUCCESS; // we do not want to suppress further executions of this timer
        }
    }

    private ListenableFuture<?> loadCheckpoint() {
        final EventKey key = EventKey.of(epoch(), -1, epoch(), "");
        final ListenableFuture<Event> f = dm.getAsync(key, fullSelector(key));
        addCallback(f, new FutureCallback<Event>() {
            @Override
            public void onSuccess(Event result) {
                if (result == null)
                    L.info("no checkpoint to load");
                else if (result.getPayload() != null) {
                    on(",").split(result.getPayload()).forEach(s -> {
                        final List<String> split = Lists.newArrayList(on("/").split(s));
                        shards.put(ZonedDateTime.parse(split.get(0)), Integer.parseInt(split.get(1)), BucketStatus.valueOf(split.get(2)));
                    });
                    L.info("loaded the checkpoint: " + shards);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                L.error("could not load the previously saved bucket status checkpoint", t);
            }
        });
        return f;
    }
}
