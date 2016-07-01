package com.walmartlabs.components.scheduler.core;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.walmartlabs.components.scheduler.model.Bucket.BucketStatus;

import java.io.IOException;
import java.time.ZonedDateTime;

import static com.walmartlabs.components.scheduler.core.ObjectFactory.OBJECT_ID.SHARD_STATUS;
import static com.walmartlabs.components.scheduler.core.ObjectFactory.SCHEDULER_FACTORY_ID;
import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.ofInstant;

/**
 * Created by smalik3 on 6/24/16
 */
public class ShardStatus implements IdentifiedDataSerializable {

    private ZonedDateTime bucketId;
    private int shard;
    private BucketStatus status;

    public ShardStatus(ZonedDateTime bucketId, int shard, BucketStatus status) {
        this.bucketId = bucketId;
        this.shard = shard;
        this.status = status;
    }

    public ShardStatus() {
    }

    public ZonedDateTime getBucketId() {
        return bucketId;
    }

    public void setBucketId(ZonedDateTime bucketId) {
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
        out.writeLong(bucketId.toInstant().toEpochMilli());
        out.writeInt(shard);
        out.writeByte(status.ordinal());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        bucketId = ofInstant(ofEpochMilli(in.readLong()), UTC);
        shard = in.readInt();
        status = BucketStatus.values()[in.readByte()];
    }

    @Override
    public String toString() {
        return format("%s[%d]=%s", bucketId, shard, status);
    }
}
