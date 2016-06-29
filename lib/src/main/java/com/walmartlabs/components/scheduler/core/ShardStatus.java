package com.walmartlabs.components.scheduler.core;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.walmartlabs.components.scheduler.model.Bucket.BucketStatus;

import java.io.IOException;

import static com.walmartlabs.components.scheduler.core.ObjectFactory.OBJECT_ID.SHARD_STATUS;
import static com.walmartlabs.components.scheduler.core.ObjectFactory.SCHEDULER_FACTORY_ID;
import static java.lang.String.format;

/**
 * Created by smalik3 on 6/24/16
 */
public class ShardStatus implements IdentifiedDataSerializable {

    private long bucketId;
    private int shard;
    private BucketStatus status;

    public ShardStatus(long bucketId, int shard, BucketStatus status) {
        this.bucketId = bucketId;
        this.shard = shard;
        this.status = status;
    }

    public ShardStatus() {
    }

    public long getBucketId() {
        return bucketId;
    }

    public void setBucketId(long bucketId) {
        this.bucketId = bucketId;
    }

    public int getShard() {
        return shard;
    }

    public void setShard(int shard) {
        this.shard = shard;
    }

    public BucketStatus getStatus() {
        return status;
    }

    public void setStatus(BucketStatus status) {
        this.status = status;
    }

    @Override
    public int getFactoryId() {
        return SCHEDULER_FACTORY_ID;
    }

    @Override
    public int getId() {
        return SHARD_STATUS.ordinal();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(bucketId);
        out.writeInt(shard);
        out.writeByte(status.ordinal());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        bucketId = in.readLong();
        shard = in.readInt();
        status = BucketStatus.values()[in.readByte()];
    }

    @Override
    public String toString() {
        return format("%d[%d]=%s", bucketId, shard, status);
    }
}
