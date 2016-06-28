package com.walmartlabs.components.scheduler.core;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.model.Event;
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.core.SpringContext.spring;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmartlabs.components.scheduler.core.ObjectFactory.OBJECT_ID.BULK_EVENT_TASK;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.ERROR;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.utc;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.tuple.Pair.of;

/**
 * Created by smalik3 on 6/16/16
 */
public class BulkShardTask implements Runnable, Callable<ShardStatusList>, IdentifiedDataSerializable, HazelcastInstanceAware {

    private static final Logger L = Logger.getLogger(BulkShardTask.class);

    private Collection<Pair<Long, Integer>> shards;

    public BulkShardTask(Collection<Pair<Long, Integer>> shards) {
        this.shards = shards;
    }

    public BulkShardTask() {
    }

    private static final ListenableFuture<List<ShardStatus>> NO_OP = immediateFuture(new ArrayList<>());

    private HazelcastInstance hz;

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }

    @Override
    public ShardStatusList call() throws Exception {
        try {
            return new ShardStatusList(execute().get());
        } catch (Exception e) {
            final Throwable cause = getRootCause(e);
            L.error("error in processing events", cause);
            throw new RuntimeException(cause);
        }
    }

    private ListenableFuture<List<ShardStatus>> execute() {
        if (shards == null || shards.isEmpty()) {
            return NO_OP;
        }
        final List<String> buckets = shards.stream().map(p -> utc(p.getLeft()).toString() + "/" + p.getRight()).collect(toList());
        L.debug(format("%s, executing bulk event task for buckets/shards on node: %s", buckets, hz.getCluster().getLocalMember().getSocketAddress()));
        final DataManager<?, ?> dm = spring().getBean(DataManager.class);
        @SuppressWarnings("unchecked")
        final EventProcessor<Event> ep = spring().getBean(ProcessorRegistry.class);
        final int maxEvents = PROPS.getInteger("event.max.events.in.memory", 10000);
        final int fetchSizeHint = maxEvents / shards.size();
        return allAsList(shards.stream().map(p -> {
            try {
                final ListenableFuture<ShardStatus> f = new ShardTask(p.getLeft(), p.getRight(), dm, ep, fetchSizeHint).call();
                addCallback(f, new FutureCallback<ShardStatus>() {
                    @Override
                    public void onSuccess(ShardStatus result) {
                        L.info(format("%s, shard processed, bucket: %d, shard: %d", utc(p.getLeft()), p.getLeft(), p.getRight()));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        L.error(format("%s, error in executing shard: bucket: %d, shard: %d", utc(p.getLeft()), p.getLeft(), p.getRight()), t);
                    }
                });
                return catching(f, Throwable.class, t -> {
                    L.error(format("%s, error in executing shard, returning a ERROR status bucket: %d, shard: %d", utc(p.getLeft()), p.getLeft(), p.getRight()), t);
                    return new ShardStatus(p.getLeft(), p.getRight(), ERROR);
                });
            } catch (Exception ex) {
                L.error(format("%s, error in submitting shard for execution: bucket: %d, shard: %d", utc(p.getLeft()), p.getLeft(), p.getRight()), ex);
                return immediateFuture(new ShardStatus(p.getLeft(), p.getRight(), ERROR));
            }
        }).collect(toList()));
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
        return BULK_EVENT_TASK.ordinal();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(shards.size());
        shards.forEach(i -> {
            try {
                out.writeLong(i.getLeft());
                out.writeInt(i.getRight());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        shards = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shards.add(of(in.readLong(), in.readInt()));
        }
    }
}
