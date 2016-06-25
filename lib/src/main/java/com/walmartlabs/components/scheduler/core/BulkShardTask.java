package com.walmartlabs.components.scheduler.core;

import com.google.common.util.concurrent.Futures;
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

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.walmart.gmp.ingestion.platform.framework.core.SpringContext.spring;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmartlabs.components.scheduler.core.ObjectFactory.OBJECT_ID.BULK_EVENT_TASK;
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
        return successfulAsList(shards.stream().map(p -> {
            try {
                return new ShardTask(p.getLeft(), p.getRight(), dm, ep).call();
            } catch (Exception ex) {
                return Futures.<ShardStatus>immediateFailedFuture(ex);
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
