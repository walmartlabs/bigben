package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.KeyMapping;
import com.walmart.gmp.ingestion.platform.framework.data.core.MutableEntity;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Id;

import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;

/**
 * Created by smalik3 on 3/29/16
 */
@Entity(table = "event_lookup")
@KeyMapping(keyClass = String.class, entityClass = EventLookup.class, version = V1)
public class EventLookupDO implements EventLookup, MutableEntity<String> {

    @Id(name = "event_id")
    @Column(name = "event_id")
    private String id;

    @Column(name = "bucket_id")
    private long bucketId;

    @Column
    private int shard;

    public long getBucketId() {
        return bucketId;
    }

    public void setBucketId(long bucketId) {
        this.bucketId = bucketId;
    }

    @Override
    public int getShard() {
        return shard;
    }

    @Override
    public void setShard(int shard) {
        this.shard = shard;
    }

    @Override
    public void id(String s) {
        this.id = s;
    }

    @Override
    public void key(Object key) {
        this.id = key.toString();
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public Object key() {
        return id();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "EventLookupDO{" +
                "id='" + id + '\'' +
                ", bucketId=" + bucketId +
                ", shard=" + shard +
                '}';
    }
}
