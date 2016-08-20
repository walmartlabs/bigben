package com.walmartlabs.components.scheduler.core;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.walmart.gmp.ingestion.platform.framework.shutdown.SupportsShutdown;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmartlabs.components.scheduler.entities.Bucket;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.shutdown.ShutdownRegistry.SHUTDOWN_REGISTRY;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.retryableExceptions;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmartlabs.components.scheduler.core.BucketManager.createEmptyBucket;
import static com.walmartlabs.components.scheduler.core.ScheduleScanner.SCHEDULER;
import static java.lang.String.format;
import static java.time.ZonedDateTime.now;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by smalik3 on 7/5/16
 */
class BucketsLoader implements SupportsShutdown {

    private static final Logger L = Logger.getLogger(BucketsLoader.class);

    private final int lookbackRange;
    private final int fetchSize;
    private final Consumer<Bucket> consumer;
    private final Predicate<ZonedDateTime> predicate;
    private final int bucketWidth;
    private final DataManager<ZonedDateTime, Bucket> dataManager;
    private final ZonedDateTime bucketId;
    private final int waitInterval;
    private final TaskExecutor taskExecutor = new TaskExecutor(retryableExceptions);
    private static final Selector<ZonedDateTime, Bucket> SELECTOR = fullSelector(now());
    private final AtomicReference<ListenableScheduledFuture<?>> runningJob = new AtomicReference<>();

    BucketsLoader(int lookbackRange, int fetchSize, Consumer<Bucket> consumer,
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
        SHUTDOWN_REGISTRY.register(this);
    }

    void start() {
        L.info(format("starting the background load of buckets at a rate of %d buckets per %d seconds until %d buckets are loaded", fetchSize, waitInterval, lookbackRange));
        runningJob.set(SCHEDULER.schedule(() -> load(0), 0, SECONDS));
    }

    private void load(int fromIndex) {
        if (fromIndex >= lookbackRange) {
            L.info("lookback range reached, no more buckets will be loaded in background");
            return;
        }
        try {
            L.info("initiating background load of buckets from index: " + fromIndex);
            final AtomicReference<Integer> currentBucketIndex = new AtomicReference<>();
            final AtomicBoolean atLeastOne = new AtomicBoolean();
            for (int i = 1; i <= fetchSize; i++) {
                final int bucketIndex = fromIndex + i;
                if (bucketIndex <= lookbackRange) {
                    currentBucketIndex.set(bucketIndex);
                    final ZonedDateTime bId = bucketId.minusSeconds(bucketIndex * bucketWidth);
                    if (!predicate.test(bId)) {
                        atLeastOne.set(true);
                        L.info(format("loading bucket: %s, failures will be retried %d times every %d seconds", bId, lookbackRange - bucketIndex + 1, bucketWidth));
                        addCallback(taskExecutor.async(() -> dataManager.getAsync(bId, SELECTOR), "bucket-load:" + bId,
                                lookbackRange - bucketIndex + 1, bucketWidth, 1, SECONDS), new FutureCallback<Bucket>() {
                            @Override
                            public void onSuccess(Bucket b) {
                                L.info(format("bucket %s loaded successfully", bId));
                                consumer.accept(b == null ? createEmptyBucket(bId) : b);
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                L.error(format("error in loading bucket %s, system is giving up", bId), getRootCause(t));
                            }
                        });
                    } else {
                        L.info(format("bucket %s already loaded, skipping...", bId));
                    }
                } else {
                    L.info("no more buckets to load, look back range reached");
                    break;
                }
            }
            runningJob.set(SCHEDULER.schedule(() -> load(currentBucketIndex.get()), !atLeastOne.get() ? 0 : waitInterval, SECONDS));
        } catch (Exception e) {
            L.error("could not load buckets: " + this, e);
        }
    }

    @Override
    public String name() {
        return "BucketLoader";
    }

    @Override
    public int priority() {
        return 0;
    }

    @Override
    public ListenableFuture<?> shutdown() {
        if (runningJob.get() != null)
            runningJob.get().cancel(true);
        return runningJob.get();
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
