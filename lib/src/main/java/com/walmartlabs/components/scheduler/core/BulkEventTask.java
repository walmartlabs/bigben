package com.walmartlabs.components.scheduler.core;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.model.Bucket.BucketStatus;
import com.walmartlabs.components.scheduler.model.Event;
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static com.beust.jcommander.internal.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.SpringContext.spring;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.utc;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Created by smalik3 on 6/16/16
 */
public class BulkEventTask implements Runnable, Callable<Map<Long, BucketStatus>>, IdentifiedDataSerializable {

    private static final Logger L = Logger.getLogger(BulkEventTask.class);

    private Map<Long, Set<Integer>> shards;

    public BulkEventTask(Map<Long, Set<Integer>> shards) {
        this.shards = shards;
    }

    public BulkEventTask() {
    }

    private static final ListenableFuture<Map<Long, BucketStatus>> DONE = immediateFuture(new HashMap<>());

    @Override
    public Map<Long, BucketStatus> call() throws Exception {
        final Map<Long, BucketStatus> status;
        try {
            status = execute().get();
        } catch (Exception e) {
            final Throwable cause = getRootCause(e);
            L.error("error in processing events", cause);
            throw new RuntimeException(cause);
        }
        return status;
    }

    private ListenableFuture<Map<Long, BucketStatus>> execute() {
        if (shards == null || shards.isEmpty()) {
            return DONE;
        }
        final List<String> buckets = shards.keySet().stream().map(millis -> utc(millis).toString()).collect(toList());
        L.debug(format("%s, executing bulk event task for buckets", buckets));
        final DataManager<?, ?> dm = spring().getBean(DataManager.class);
        @SuppressWarnings("unchecked")
        final EventProcessor<Event> ep = spring().getBean(ProcessorRegistry.class);
        final List<Entry<Long, Set<Integer>>> entries = newArrayList(shards.entrySet());
        return transform(successfulAsList(entries.stream().map(e -> {
            try {
                return new EventTask(e.getKey(), e.getValue(), dm, ep).call();
            } catch (Exception ex) {
                return Futures.<BucketStatus>immediateFailedFuture(ex);
            }
        }).collect(toList())), (Function<List<BucketStatus>, Map<Long, BucketStatus>>) l -> {
            L.info(format("%s, shards done, input: %s, output: %s", buckets, entries, l));
            final AtomicInteger index = new AtomicInteger();
            return entries.stream().collect(toMap((java.util.function.Function<Entry<Long, Set<Integer>>, Long>)
                    Entry::getKey, e -> l.get(index.getAndIncrement())));
        });

    }

    @Override
    public void run() {
        execute();
    }

    @Override
    public int getFactoryId() {
        return ObjectFactory.SCHEDULER_FACTORY_ID;
    }

    @Override
    public int getId() {
        return ObjectFactory.OBJECT_ID.BULK_EVENT_TASK.ordinal();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(shards.size());
        shards.forEach((k, v) -> {
            try {
                out.writeLong(k);
                out.writeInt(v.size());
                for (Integer s : v) {
                    out.writeInt(s);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        shards = new HashMap<>();
        for (int i = 0; i < size; i++) {
            final long key = in.readLong();
            final int shardSize = in.readInt();
            final Set<Integer> ss = new HashSet<>();
            for (int j = 0; j < shardSize; j++) {
                ss.add(in.readInt());
            }
            shards.put(key, ss);
        }
    }
}
