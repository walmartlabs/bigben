package com.walmartlabs.components.scheduler.core;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmartlabs.components.scheduler.entities.Bucket;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventDO.EventKey;
import com.walmartlabs.components.scheduler.entities.Status;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmartlabs.components.scheduler.entities.Status.*;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.nowUTC;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
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
    private final DataManager<EventKey, Event> eventDataManager;
    private final int lookbackRange;
    private final Table<ZonedDateTime, Integer, Status> shards = TreeBasedTable.create();
    private final int bucketWidth;
    private volatile BucketsLoader bucketsLoader;
    private final StatusSyncer statusSyncer;
    private final CheckpointHelper checkpointHelper;

    @SuppressWarnings("unchecked")
    public BucketManager(int maxBuckets, int maxProcessingTime, DataManager<?, ?> dm, int bucketWidth,
                         int checkpointInterval, TimeUnit checkpointUnit, int lookbackRange) {
        this.maxBuckets = maxBuckets;
        this.maxProcessingTime = maxProcessingTime;
        this.dataManager = (DataManager<ZonedDateTime, Bucket>) dm;
        this.eventDataManager = (DataManager<EventKey, Event>) dm;
        this.lookbackRange = lookbackRange;
        final AtomicInteger index = new AtomicInteger();
        service = listeningDecorator(newScheduledThreadPool(getRuntime().availableProcessors(),
                (r) -> new Thread(r, "BucketManagerInternalScheduler#" + index.getAndIncrement())));
        this.bucketWidth = bucketWidth;
        checkpointHelper = new CheckpointHelper(this);
        statusSyncer = new StatusSyncer(dataManager, eventDataManager);
        L.info(format("saving checkpoint every %d %s", checkpointInterval, checkpointUnit));
        service.scheduleAtFixedRate(this::saveCheckpoint, checkpointInterval, checkpointInterval, checkpointUnit);
        try {
            L.info("loading the previously saved checkpoint, if any");
            transform(checkpointHelper.loadCheckpoint(shards), (Function<Object, Object>) $ -> {
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
    }

    private final Consumer<List<Bucket>> consumer = l -> {
        synchronized (BucketManager.this) {
            l.forEach(b -> {
                if (b != null && !shards.containsRow(b.id())) {
                    if (b.getCount() == 0) {
                        shards.put(b.id(), -1, EMPTY);
                    } else if (b.getCount() > 0 && PROCESSED.name().equals(b.getStatus())) {
                        shards.put(b.id(), -1, PROCESSED);
                    } else if (b.getCount() > 0 && !PROCESSED.name().equals(b.getStatus())) {
                        int shardSize = PROPS.getInteger("event.shard.size", 1000);
                        final int count = (int) b.getCount();
                        final int numShards = count % shardSize == 0 ? count / shardSize : count / shardSize + 1;
                        for (int i = 0; i < numShards; i++) {
                            if (!shards.contains(b.id(), i))
                                shards.put(b.id(), i, UN_PROCESSED);
                        }
                    }
                }
            });
        }
    };

    private static final Selector<ZonedDateTime, Bucket> selector = fullSelector(nowUTC());

    synchronized ListenableFuture<Multimap<ZonedDateTime, Integer>> getProcessableShardsForOrBefore(ZonedDateTime bucketId) {
        if (bucketsLoader == null) {
            L.info("starting the background load of previous buckets");
            final Integer fetchSize = PROPS.getInteger("buckets.background.load.fetch.size", 10);
            bucketsLoader = new BucketsLoader(lookbackRange, fetchSize, consumer, shards::containsRow, bucketWidth, dataManager, bucketId, service);
            bucketsLoader.start();
        }

        final ListenableFuture<Bucket> f = dataManager.getAsync(bucketId, selector);
        final Multimap<ZonedDateTime, Integer> processableShards = HashMultimap.create();
        addCallback(f, new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(Bucket bucket) {
                consumer.accept(newArrayList(bucket));
                shards.cellSet().stream().
                        filter(cell -> cell.getValue() != EMPTY && cell.getValue() != PROCESSED && cell.getValue() != PROCESSING).
                        forEach(cell -> processableShards.put(cell.getRowKey(), cell.getColumnKey()));
                L.info(format("processable shards at bucket: %s, are => %s", bucketId, processableShards));
                if (!processableShards.containsKey(bucketId)) {
                    L.info(format("no events in the bucket: %s, marking it %s", bucketId, EMPTY));
                    shards.put(bucketId, -1, EMPTY);
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
        for (Pair<ZonedDateTime, Integer> pair : pairs) {
            shards.put(pair.getLeft(), pair.getRight(), PROCESSING);
        }
        purgeIfNeeded();
        startShardsTimer(pairs);
    }

    ListenableScheduledFuture<?> startShardsTimer(Collection<Pair<ZonedDateTime, Integer>> pairs) {
        final List<String> shards = pairs.stream().sorted().map(p -> p.getLeft() + "[" + p.getRight() + "]").collect(toList());
        L.info(format("starting bulk timer for shards: %s", shards));
        return service.schedule(() -> checkShardsStatus(pairs, shards), maxProcessingTime, SECONDS);
    }

    private synchronized void checkShardsStatus(Collection<Pair<ZonedDateTime, Integer>> pairs, List<String> shards) {
        try {
            for (Pair<ZonedDateTime, Integer> pair : pairs) {
                final ZonedDateTime bucketId = pair.getLeft();
                final Integer shard = pair.getRight();
                final Status status = this.shards.get(bucketId, shard);
                if (status != null && status == PROCESSING) {
                    L.warn(format("bulk timer for shard: %s[%d] expired, marking it %s", bucketId, shard, UN_PROCESSED));
                    this.shards.put(bucketId, shard, UN_PROCESSED);
                }
            }
        } catch (Exception e) {
            L.error(format("error in timing out the shards for processing, shards: %s", shards), e);
        }
    }

    synchronized void shardDone(ZonedDateTime bucketId, Integer shard, Status shardStatus) {
        switch (shardStatus) {
            case PROCESSED:
                L.info(format("shard: %s[%s] finished successfully", bucketId, shard));
                shards.remove(bucketId, shard);
                if (!shards.containsRow(bucketId)) {
                    L.info(format("no more shards to be processed for bucket: %s, marking it %s", bucketId, PROCESSED));
                    bucketProcessed(bucketId, PROCESSED);
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

    private static final ListenableFuture<Bucket> NO_OP = immediateFuture(null);

    synchronized ListenableFuture<Bucket> bucketProcessed(ZonedDateTime bucketId, Status status) {
        if (shards.get(bucketId, -1) == status)
            return NO_OP;
        L.info(format("marking bucket %s as " + status, bucketId));
        final Map<Integer, Status> shards = this.shards.rowMap().get(bucketId);
        if (shards != null) {
            final Set<Integer> dup = new HashSet<>(shards.keySet());
            for (Integer shardKey : dup) {
                this.shards.remove(bucketId, shardKey);
            }
        }
        this.shards.put(bucketId, -1, status);
        return statusSyncer.syncBucket(bucketId, status);
    }

    private void purgeIfNeeded() {
        try {
            final List<ZonedDateTime> bucketIds = reverse(newArrayList(new TreeSet<>(shards.rowKeySet())));
            final List<Pair<ZonedDateTime, Integer>> purged = new ArrayList<>();
            L.debug("checking buckets for purging from shards: " + shards);
            if (bucketIds.size() > maxBuckets) {
                for (int i = maxBuckets; i < bucketIds.size(); i++) {
                    final ZonedDateTime bucketId = bucketIds.get(i);
                    final Map<Integer, Status> statusByShard = shards.row(bucketId);
                    L.info(format("purging the following shards for bucket %s, => %s", bucketId, statusByShard));
                    final Set<Status> statuses = new HashSet<>(statusByShard.values());
                    if (statuses.contains(ERROR)) {
                        statusSyncer.syncBucket(bucketId, ERROR);
                    } else if (statuses.contains(UN_PROCESSED) || statuses.contains(PROCESSING)) {
                        statusSyncer.syncBucket(bucketId, UN_PROCESSED);
                    } else if (statuses.size() == 1 && statuses.contains(PROCESSED)) {
                        statusSyncer.syncBucket(bucketId, PROCESSED);
                    }
                    final Set<Integer> dups = new HashSet<>(statusByShard.keySet());
                    for (Integer shard : dups) {
                        shards.remove(bucketId, shard);
                        purged.add(of(bucketId, shard));
                    }
                }
            }
            if (!purged.isEmpty()) {
                L.info("purged shards: " + purged);
                L.debug("shards after purging: " + shards);
            } else L.info("nothing to purge");
        } catch (Exception e) {
            L.error("error in purging shards: " + shards, e);
        }
    }

    synchronized void saveCheckpoint() {
        purgeIfNeeded();
        checkpointHelper.saveCheckpoint(shards);
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
}
