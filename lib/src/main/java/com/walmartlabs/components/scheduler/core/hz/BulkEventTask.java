package com.walmartlabs.components.scheduler.core.hz;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.core.EventProcessor;
import com.walmartlabs.components.scheduler.model.Bucket.BucketStatus;
import com.walmartlabs.components.scheduler.model.Event;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.beust.jcommander.internal.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static com.walmart.gmp.ingestion.platform.framework.core.SpringContext.spring;
import static java.util.stream.Collectors.toList;

/**
 * Created by smalik3 on 6/16/16
 */
public class BulkEventTask implements Runnable {

    private static final Logger L = Logger.getLogger(BulkEventTask.class);

    private final Map<Long, Set<Integer>> shards;

    public BulkEventTask(Map<Long, Set<Integer>> shards) {
        this.shards = shards;
    }

    @Override
    public void run() {
        final DataManager<?, ?> dm = spring().getBean(DataManager.class);
        @SuppressWarnings("unchecked")
        final EventProcessor<Event> ep = spring().getBean(EventProcessor.class);
        final List<Entry<Long, Set<Integer>>> entries = newArrayList(shards.entrySet());
        transform(successfulAsList(entries.stream().map(e -> {
            try {
                return new EventTask(e.getKey(), e.getValue(), dm, ep).call();
            } catch (Exception ex) {
                return Futures.<BucketStatus>immediateFailedFuture(ex);
            }
        }).collect(toList())), (Function<List<BucketStatus>, List<BucketStatus>>) l -> {
            L.info("shards done, input: " + entries + ", output: " + l);
            return l;
        });
    }
}
