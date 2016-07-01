package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.KeyMapping;
import com.walmart.gmp.ingestion.platform.framework.data.core.MutableEntity;
import info.archinnov.achilles.annotations.*;

import java.io.Serializable;
import java.time.ZonedDateTime;

import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;

/**
 * Created by smalik3 on 3/18/16
 */
@Entity(table = "event_buckets")
@KeyMapping(keyClass = Long.class, entityClass = Bucket.class, version = V1)
public class BucketDO implements Bucket, MutableEntity<ZonedDateTime>, Serializable {

    @PartitionKey
    @Id(name = "id")
    @Column(name = "id")
    @TypeTransformer(valueCodecClass = ZonedDateTimeToDate.class)
    private ZonedDateTime id;

    @Column
    private String status;

    @Column
    private long count;

    @Column(name = "processed_at")
    @TypeTransformer(valueCodecClass = ZonedDateTimeToDate.class)
    private ZonedDateTime processedAt;

    public ZonedDateTime getId() {
        return id;
    }

    public void setId(ZonedDateTime id) {
        this.id = id;
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
    public ZonedDateTime getProcessedAt() {
        return processedAt;
    }

    @Override
    public void setProcessedAt(ZonedDateTime processedAt) {
        this.processedAt = processedAt;
    }

    @Override
    public ZonedDateTime id() {
        return id;
    }

    @Override
    public Object key() {
        return id;
    }

    @Override
    public void id(ZonedDateTime id) {
        this.id = id;
    }

    @Override
    public void key(Object o) {
        this.id = (ZonedDateTime) o;
    }

    @Override
    public String toString() {
        return "BucketDO{" +
                "id=" + id +
                ", status='" + status + '\'' +
                ", count=" + count +
                ", processedAt=" + processedAt +
                '}';
    }
}
