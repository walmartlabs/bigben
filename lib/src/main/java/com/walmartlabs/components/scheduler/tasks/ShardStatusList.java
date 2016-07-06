package com.walmartlabs.components.scheduler.tasks;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.walmartlabs.components.scheduler.entities.ObjectFactory.OBJECT_ID.SHARD_STATUS_LIST;
import static com.walmartlabs.components.scheduler.entities.ObjectFactory.SCHEDULER_FACTORY_ID;

/**
 * Created by smalik3 on 6/25/16
 */
public class ShardStatusList implements IdentifiedDataSerializable {

    private List<ShardStatus> list;

    public ShardStatusList(List<ShardStatus> list) {
        this.list = list;
    }

    public ShardStatusList() {
    }

    public List<ShardStatus> getList() {
        return list;
    }

    @Override
    public int getFactoryId() {
        return SCHEDULER_FACTORY_ID;
    }

    @Override
    public int getId() {
        return SHARD_STATUS_LIST.ordinal();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(list.size());
        for (ShardStatus shardStatus : list) {
            out.writeObject(shardStatus);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(in.readObject());
        }
    }

    @Override
    public String toString() {
        return list.toString();
    }
}
