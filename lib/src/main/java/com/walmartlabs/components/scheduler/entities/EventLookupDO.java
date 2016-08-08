package com.walmartlabs.components.scheduler.entities;

import com.walmart.gmp.ingestion.platform.framework.data.core.KeyMapping;
import com.walmart.gmp.ingestion.platform.framework.data.core.MutableEntity;
import com.walmartlabs.components.scheduler.entities.codecs.ZonedDateTimeToDate;
import info.archinnov.achilles.annotations.*;

import java.time.ZonedDateTime;

import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;

/**
 * Created by smalik3 on 3/29/16
 */
@Entity(table = "event_lookup")
@KeyMapping(keyClass = EventLookupDO.EventLookupKey.class, entityClass = EventLookup.class, version = V1)
public class EventLookupDO implements EventLookup, MutableEntity<EventLookupDO.EventLookupKey> {

    @EmbeddedId
    private EventLookupKey id;

    @Column(name = "bucket_id")
    @TypeTransformer(valueCodecClass = ZonedDateTimeToDate.class)
    private ZonedDateTime bucketId;

    @Column
    private int shard;

    @Column(name = "event_time")
    @TypeTransformer(valueCodecClass = ZonedDateTimeToDate.class)
    private ZonedDateTime eventTime;

    @Column(name = "event_id")
    private String eventId;

    @Column(name = "created_at")
    @TypeTransformer(valueCodecClass = ZonedDateTimeToDate.class)
    private ZonedDateTime createdAt;

    @TypeTransformer(valueCodecClass = ZonedDateTimeToDate.class)
    @Column(name = "modified_at")
    private ZonedDateTime modifiedAt;

    @Column
    private String payload;

    public ZonedDateTime getBucketId() {
        return bucketId;
    }

    public void setBucketId(ZonedDateTime bucketId) {
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
    public void id(EventLookupKey s) {
        this.id = s;
    }

    @Override
    public void key(Object key) {
        this.id = (EventLookupKey) key;
    }

    @Override
    public EventLookupKey id() {
        return id;
    }

    @Override
    public Object key() {
        return id();
    }

    public EventLookupKey getId() {
        return id;
    }

    public void setId(EventLookupKey id) {
        this.id = id;
    }

    @Override
    public ZonedDateTime getEventTime() {
        return eventTime;
    }

    @Override
    public void setEventTime(ZonedDateTime eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String getEventId() {
        return eventId;
    }

    @Override
    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    @Override
    public ZonedDateTime getCreatedAt() {
        return createdAt;
    }

    @Override
    public void setCreatedAt(ZonedDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public ZonedDateTime getModifiedAt() {
        return modifiedAt;
    }

    public void setModifiedAt(ZonedDateTime modifiedAt) {
        this.modifiedAt = modifiedAt;
    }

    @Override
    public String getPayload() {
        return payload;
    }

    @Override
    public void setPayload(String payload) {
        this.payload = payload;
    }

    public static class EventLookupKey {
        @PartitionKey(value = 1)
        @Column
        private String tenant;

        @PartitionKey(value = 2)
        @Column(name = "xref_id")
        private String xrefId;

        public String getXrefId() {
            return xrefId;
        }

        public void setXrefId(String xrefId) {
            this.xrefId = xrefId;
        }

        public EventLookupKey(String xrefId, String tenant) {
            this.xrefId = xrefId;
            this.tenant = tenant;
        }

        public EventLookupKey() {
        }

        public String getTenant() {
            return tenant;
        }

        public void setTenant(String tenant) {
            this.tenant = tenant;
        }

        @Override
        public String toString() {
            return "EventLookupKey{" +
                    "xrefId='" + xrefId + '\'' +
                    ", tenant='" + tenant + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "EventLookupDO{" +
                "id=" + id +
                ", bucketId=" + bucketId +
                ", shard=" + shard +
                ", eventTime=" + eventTime +
                ", eventId='" + eventId + '\'' +
                ", createdAt=" + createdAt +
                ", modifiedAt=" + modifiedAt +
                ", payload='" + payload + '\'' +
                '}';
    }
}
