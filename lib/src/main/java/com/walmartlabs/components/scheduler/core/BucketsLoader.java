package com.walmartlabs.components.scheduler.core;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmartlabs.components.scheduler.entities.Bucket;
import com.walmartlabs.components.scheduler.entities.BucketDO;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.raw;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.retryableExceptions;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmartlabs.components.scheduler.core.ScheduleScanner.SCHEDULER;
import static com.walmartlabs.components.scheduler.entities.Status.EMPTY;
import static java.lang.String.format;
import static java.time.ZonedDateTime.now;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by smalik3 on 7/5/16
 */
class BucketsLoader {

    private static final Logger L = Logger.getLogger(BucketsLoader.class);

    private final int lookbackRange;
    private final int fetchSize;
    private final Consumer<List<Bucket>> consumer;
    private final Predicate<ZonedDateTime> predicate;
    private final int bucketWidth;
    private final DataManager<ZonedDateTime, Bucket> dataManager;
    private final ZonedDateTime bucketId;
    private final int waitInterval;
    private final TaskExecutor taskExecutor = new TaskExecutor(retryableExceptions);
    private static final Selector<ZonedDateTime, Bucket> SELECTOR = fullSelector(now());

    BucketsLoader(int lookbackRange, int fetchSize, Consumer<List<Bucket>> consumer,
                  Predicate<ZonedDateTime> predicate, int bucketWidth,
                  DataManager<ZonedDateTime, Bucket> dataManager, ZonedDateTime bucketId) {
        this.lookbackRange = lookbackRange;
        this.fetchSize = fetchSize;
        this.consumer = consumer;
        this.predicate = predicate;
        this.bucketWidth = bucketWidth;
        this.dataManager = dataManager;
        this.bucketId = bucketId;
        waitInterval = PROPS.getInteger("buckets.background.load.wait.interval.seconds", 15);
    }

    void start() {
        L.info(format("starting the background load of buckets at a rate of %d buckets per %d seconds until %d buckets are loaded", fetchSize, waitInterval, lookbackRange));
        SCHEDULER.schedule(() -> load(0), 0, SECONDS);
    }

    private void load(int fromIndex) {
        if (fromIndex >= lookbackRange) {
            L.info("lookback range reached, no more buckets will be loaded in background");
            return;
        }
        try {
            L.info("initiating background load of buckets from index: " + fromIndex);
            final List<ListenableFuture<Bucket>> futures = new ArrayList<>();
            final AtomicReference<Integer> currentBucketIndex = new AtomicReference<>();
            for (int i = 1; i <= fetchSize; i++) {
                final int bucketIndex = fromIndex + i;
                if (bucketIndex <= lookbackRange) {
                    currentBucketIndex.set(bucketIndex);
                    final ZonedDateTime bId = bucketId.minusSeconds(bucketIndex * bucketWidth);
                    if (!predicate.test(bId)) {
                        L.info("loading bucket: " + bId);
                        futures.add(catching(transform(dataManager.getAsync(bId, SELECTOR),
                                new Function<Bucket, Bucket>() {
                                    @Override
                                    public Bucket apply(Bucket b) {
                                        return b == null ? createEmptyBucket(bId) : raw(b);
                                    }
                                }), Exception.class, ex -> {
                            L.error(format("error in fetching bucket: %s, system will retry for %d times before giving up", bId, (lookbackRange - fromIndex + 1)), ex);
                            taskExecutor.async(() -> {
                                L.info("retrying the bucket re-load: " + bId);
                                return transform(dataManager.getAsync(bId, SELECTOR), (Function<Bucket, Bucket>) b -> {
                                    consumer.accept(singletonList(b));
                                    return b;
                                });
                            }, "bucket-load-retry-" + bId, lookbackRange - fromIndex + 1, bucketWidth, 1, MINUTES);
                            return createEmptyBucket(bId);
                        }));
                    } else {
                        L.info(format("bucket %s already loaded, skipping...", bId));
                    }
                } else {
                    L.info("no more buckets to load, look back range reached");
                    break;
                }
            }
            addCallback(successfulAsList(futures), new FutureCallback<List<Bucket>>() {
                @Override
                public void onSuccess(List<Bucket> buckets) {
                    L.info("loaded buckets: " + buckets);
                    consumer.accept(buckets);
                    SCHEDULER.schedule(() -> load(currentBucketIndex.get()), futures.isEmpty() ? 0 : waitInterval, SECONDS);
                }

                @Override
                public void onFailure(Throwable t) {
                    L.error("could not load buckets, trying again: " + this, t);
                    SCHEDULER.schedule(() -> load(fromIndex), waitInterval, SECONDS);
                }
            });
        } catch (Exception e) {
            L.error("could not load buckets: " + this, e);
        }
    }

    private Bucket createEmptyBucket(ZonedDateTime bId) {
        final BucketDO bucketDO = new BucketDO();
        bucketDO.setId(bId);
        bucketDO.setStatus(EMPTY.name());
        bucketDO.setCount(0);
        return bucketDO;
    }

    @Override
    public String toString() {
        return "BucketsLoader{" +
                "lookbackRange=" + lookbackRange +
                ", fetchSize=" + fetchSize +
                ", consumer=" + consumer +
                ", predicate=" + predicate +
                ", bucketWidth=" + bucketWidth +
                ", dataManager=" + dataManager +
                ", bucketId=" + bucketId +
                '}';
    }
}
