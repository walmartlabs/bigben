package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.KeyMapping;
import com.walmart.gmp.ingestion.platform.framework.data.core.MutableEntity;
import info.archinnov.achilles.annotations.*;

import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static java.lang.String.format;

/**
 * Created by smalik3 on 3/8/16
 */
@Entity(table = "events")
@KeyMapping(keyClass = EventDO.EventKey.class, entityClass = Event.class, version = V1)
public class EventDO implements Event, MutableEntity<EventDO.EventKey> {

    @EmbeddedId
    private EventKey eventKey;

    @Column
    private String status;

    @Column
    private String error;

    @Column
    private String tenant;

    @Column(name = "xref_id")
    private String xrefId;

    @Column
    private String payload;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public void id(EventKey eventKey) {
        this.eventKey = eventKey;
    }

    @Override
    public void key(Object o) {
        this.eventKey = (EventKey) o;
    }

    @Override
    public EventKey id() {
        return eventKey;
    }

    @Override
    public Object key() {
        return eventKey;
    }

    public EventKey getEventKey() {
        return eventKey;
    }

    public void setEventKey(EventKey eventKey) {
        this.eventKey = eventKey;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    @Override
    public String getXrefId() {
        return xrefId;
    }

    @Override
    public void setXrefId(String xrefId) {
        this.xrefId = xrefId;
    }

    @Override
    public String getPayload() {
        return payload;
    }

    @Override
    public void setPayload(String payload) {
        this.payload = payload;
    }

    private long millis;

    @Override
    public void setProcessedAt(long millis) {
        this.millis = millis;
    }

    @Override
    public long getProcessedAt() {
        return millis;
    }

    public static class EventKey {
        @PartitionKey
        @Column(name = "bucket_id")
        private long bucketId;

        @PartitionKey(2)
        @Column(name = "shard")
        private int shard;

        @ClusteringColumn
        @Column(name = "event_time")
        private long eventTime;

        @ClusteringColumn(2)
        @Column(name = "event_id")
        private String eventId;

        public static EventKey of(long bucketId, int shard, long eventTime, String eventId) {
            final EventKey eventKey = new EventKey();
            eventKey.bucketId = bucketId;
            eventKey.eventTime = eventTime;
            eventKey.eventId = eventId;
            eventKey.shard = shard;
            return eventKey;
        }

        public long getBucketId() {
            return bucketId;
        }

        public void setBucketId(long bucketId) {
            this.bucketId = bucketId;
        }

        public String getEventId() {
            return eventId;
        }

        public void setEventId(String eventId) {
            this.eventId = eventId;
        }

        public long getEventTime() {
            return eventTime;
        }

        public void setEventTime(long eventTime) {
            this.eventTime = eventTime;
        }

        public int getShard() {
            return shard;
        }

        public void setShard(int shard) {
            this.shard = shard;
        }

        @Override
        public String toString() {
            return format("Event[%d/%d/%d/%s]", bucketId, shard, eventTime, eventId);
        }
    }

    @Override
    public String toString() {
        return "EventDO{" +
                "eventKey=" + eventKey +
                ", status='" + status + '\'' +
                ", error='" + error + '\'' +
                ", tenant='" + tenant + '\'' +
                ", xrefId='" + xrefId + '\'' +
                ", payload='" + payload + '\'' +
                ", millis=" + millis +
                '}';
    }
}
