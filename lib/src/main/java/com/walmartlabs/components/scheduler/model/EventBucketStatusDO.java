package com.walmartlabs.components.scheduler.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.walmart.gmp.ingestion.platform.framework.data.core.KeyMapping;
import com.walmart.gmp.ingestion.platform.framework.data.core.MutableEntity;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.annotations.PartitionKey;

import java.io.IOException;
import java.util.BitSet;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static com.walmart.gmp.ingestion.platform.framework.messaging.kafka.Constants.OBJECT_MAPPER;

/**
 * Created by smalik3 on 3/18/16
 */
@Entity(table = "event_ring")
@KeyMapping(keyClass = Long.class, entityClass = EventBucketStatusEntity.class, version = V1)
public class EventBucketStatusDO implements EventBucketStatusEntity, MutableEntity<Long> {

    @PartitionKey
    @Id(name = "offset")
    @Column(name = "offset")
    private Long offset;

    @Column
    private String status;

    @Column
    private long count;

    @Column
    private String error;

    @Column(name = "failed_shards")
    private String failedShards = "";

    private int jobCount;

    private BitSet bitSet;

    public EventBucketStatusDO() {
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public String getStatus() {
        return status;
    }

    @Override
    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public void setError(String s) {
        this.error = s;
    }

    @Override
    public String getError() {
        return error;
    }

    @Override
    public void setJobCount(int jobCount) {
        this.jobCount = jobCount;
    }

    @Override
    public int getJobCount() {
        return jobCount;
    }

    @Override
    public String getFailedShards() {
        return failedShards;
    }

    @Override
    public void setFailedShards(String failedShards) {
        this.failedShards = failedShards;
        try {
            if (failedShards != null && failedShards.length() > 0) {
                long[] failedShardsArray = OBJECT_MAPPER.readValue(failedShards, new TypeReference<long[]>() {
                });
                bitSet = BitSet.valueOf(failedShardsArray);
            } else bitSet = new BitSet();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addFailedShards(Set<Integer> failedShards) {
        for (Integer failedShard : failedShards) {
            bitSet.set(failedShard);
        }
        try {
            this.failedShards = OBJECT_MAPPER.writeValueAsString(bitSet.toLongArray());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<Integer> failedShards() {
        final Set<Integer> s = newHashSet();
        for (int i = 0; i < bitSet.size(); i++) {
            if (bitSet.get(i))
                s.add(i);
        }
        return s;
    }

    @Override
    public Long id() {
        return offset;
    }

    @Override
    public Object key() {
        return offset;
    }

    @Override
    public void id(Long aLong) {
        this.offset = aLong;
    }

    @Override
    public void key(Object o) {
        this.offset = Long.parseLong(String.valueOf(o));
    }

    @Override
    public String toString() {
        return "EventBucketStatusDO{" +
                "offset=" + offset +
                ", status='" + status + '\'' +
                ", count=" + count +
                ", error='" + error + '\'' +
                '}';
    }
}
