package com.walmartlabs.components.scheduler.core;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.entities.Bucket;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Created by smalik3 on 7/5/16
 */
public class BackgroundBucketsLoader {

    private static final Logger L = Logger.getLogger(BackgroundBucketsLoader.class);

    private final int lookbackRange;
    private final int fetchSize;
    private final Consumer<List<Bucket>> consumer;
    private final Predicate<ZonedDateTime> predicate;
    private final int bucketWidth;
    private final DataManager<ZonedDateTime, Bucket> dataManager;
    private final ZonedDateTime bucketId;
    private final ListeningScheduledExecutorService service;

    public BackgroundBucketsLoader(int lookbackRange, int fetchSize, Consumer<List<Bucket>> consumer,
                                   Predicate<ZonedDateTime> predicate, int bucketWidth,
                                   DataManager<ZonedDateTime, Bucket> dataManager, ZonedDateTime bucketId,
                                   ListeningScheduledExecutorService service) {
        this.lookbackRange = lookbackRange;
        this.fetchSize = fetchSize;
        this.consumer = consumer;
        this.predicate = predicate;
        this.bucketWidth = bucketWidth;
        this.dataManager = dataManager;
        this.bucketId = bucketId;
        this.service = service;
    }

    void start() {
        service.schedule(() -> load(0), 0, SECONDS);
    }

    private void load(int fromIndex) {
        if (fromIndex > lookbackRange) {
            L.info("lookback range reached, no more buckets will be loaded in background");
        }
        try {
            final List<ListenableFuture<Bucket>> futures = new ArrayList<>();
            final AtomicReference<Integer> currentBucketIndex = new AtomicReference<>();
            for (int i = 1; i <= fetchSize; i++) {
                final int bucketIndex = fromIndex + i;
                if (bucketIndex <= lookbackRange) {
                    currentBucketIndex.set(bucketIndex);
                    final ZonedDateTime bId = bucketId.minusSeconds(bucketIndex * bucketWidth);
                    if (!predicate.test(bId)) {
                        futures.add(dataManager.getAsync(bId, fullSelector(bId)));
                    }
                }
            }
            addCallback(successfulAsList(futures), new FutureCallback<List<Bucket>>() {
                @Override
                public void onSuccess(List<Bucket> buckets) {
                    final List<Bucket> notNulls = buckets.stream().filter(b -> b != null).collect(toList());
                    L.info("loaded buckets: " + notNulls);
                    consumer.accept(notNulls);
                    service.schedule(() -> load(currentBucketIndex.get()), 0, SECONDS);
                }

                @Override
                public void onFailure(Throwable t) {
                    L.error("could not load buckets, trying again: " + this, t);
                    service.schedule(() -> load(fromIndex), 0, SECONDS);
                }
            });
        } catch (Exception e) {
            L.error("could not load buckets: " + this, e);

        }
    }

    @Override
    public String toString() {
        return "BackgroundBucketsLoader{" +
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
