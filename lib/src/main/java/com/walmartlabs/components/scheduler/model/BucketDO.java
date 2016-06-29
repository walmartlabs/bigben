package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.KeyMapping;
import com.walmart.gmp.ingestion.platform.framework.data.core.MutableEntity;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.annotations.PartitionKey;

import java.io.Serializable;
import java.util.Date;

import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static java.lang.Long.parseLong;

/**
 * Created by smalik3 on 3/18/16
 */
@Entity(table = "event_buckets")
@KeyMapping(keyClass = Long.class, entityClass = Bucket.class, version = V1)
public class BucketDO implements Bucket, MutableEntity<Long>, Serializable {

    @PartitionKey
    @Id(name = "id")
    @Column(name = "id")
    private Long id;

    @Column
    private String status;

    @Column
    private long count;

    @Column(name = "processed_at")
    private Date processedAt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
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
    public Date getProcessedAt() {
        return processedAt;
    }

    @Override
    public void setProcessedAt(Date processedAt) {
        this.processedAt = processedAt;
    }

    @Override
    public Long id() {
        return id;
    }

    @Override
    public Object key() {
        return id;
    }

    @Override
    public void id(Long aLong) {
        this.id = aLong;
    }

    @Override
    public void key(Object o) {
        this.id = parseLong(String.valueOf(o));
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
