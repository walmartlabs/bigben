package com.walmartlabs.components.scheduler.entities;

import com.walmart.gmp.ingestion.platform.framework.data.core.KeyMapping;
import com.walmart.gmp.ingestion.platform.framework.data.core.MutableEntity;
import com.walmartlabs.components.scheduler.entities.codecs.ZonedDateTimeToDate;
import info.archinnov.achilles.annotations.*;

import java.time.ZonedDateTime;

import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static java.lang.String.format;

/**
 * Created by smalik3 on 3/8/16
 */
@Entity(table = "events")
@KeyMapping(keyClass = EventDO.EventKey.class, entityClass = Event.class, version = V1)
public class EventDO implements Event, MutableEntity<EventDO.EventKey>, EventResponseMixin {

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

    @Column(name = "processed_at")
    @TypeTransformer(valueCodecClass = ZonedDateTimeToDate.class)
    private ZonedDateTime processedAt;

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

    @Override
    public ZonedDateTime getProcessedAt() {
        return processedAt;
    }

    @Override
    public void setProcessedAt(ZonedDateTime processedAt) {
        this.processedAt = processedAt;
    }

    public static class EventKey {
        @PartitionKey
        @Column(name = "bucket_id")
        @TypeTransformer(valueCodecClass = ZonedDateTimeToDate.class)
        private ZonedDateTime bucketId;

        @PartitionKey(2)
        @Column(name = "shard")
        private int shard;

        @ClusteringColumn
        @Column(name = "event_time")
        @TypeTransformer(valueCodecClass = ZonedDateTimeToDate.class)
        private ZonedDateTime eventTime;

        @ClusteringColumn(2)
        @Column(name = "event_id")
        private String eventId;

        public static EventKey of(ZonedDateTime bucketId, int shard, ZonedDateTime eventTime, String eventId) {
            final EventKey eventKey = new EventKey();
            eventKey.bucketId = bucketId;
            eventKey.eventTime = eventTime;
            eventKey.eventId = eventId;
            eventKey.shard = shard;
            return eventKey;
        }

        public ZonedDateTime getBucketId() {
            return bucketId;
        }

        public void setBucketId(ZonedDateTime bucketId) {
            this.bucketId = bucketId;
        }

        public String getEventId() {
            return eventId;
        }

        public void setEventId(String eventId) {
            this.eventId = eventId;
        }

        public ZonedDateTime getEventTime() {
            return eventTime;
        }

        public void setEventTime(ZonedDateTime eventTime) {
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
            return format("Event[%s/%d/%s/%s]", bucketId, shard, eventTime, eventId);
        }
    }

    private EventResponse eventResponse;

    @Override
    public EventResponse getEventResponse() {
        return eventResponse;
    }

    @Override
    public void setEventResponse(EventResponse eventResponse) {
        this.eventResponse = eventResponse;
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
                ", processedAt=" + getProcessedAt() +
                '}';
    }
}
