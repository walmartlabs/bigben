package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.KeyMapping;
import com.walmart.gmp.ingestion.platform.framework.data.core.MutableEntity;
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

    public static class EventLookupKey {
        @PartitionKey
        @Column(name = "xref_id")
        private String xrefId;

        @ClusteringColumn
        @Column(name = "event_id")
        private String eventId;

        public String getXrefId() {
            return xrefId;
        }

        public void setXrefId(String xrefId) {
            this.xrefId = xrefId;
        }

        public String getEventId() {
            return eventId;
        }

        public void setEventId(String eventId) {
            this.eventId = eventId;
        }

        public EventLookupKey(String xrefId, String eventId) {
            this.xrefId = xrefId;
            this.eventId = eventId;
        }

        public EventLookupKey() {
        }

        @Override
        public String toString() {
            return "EventLookupKey{" +
                    "xrefId='" + xrefId + '\'' +
                    ", eventId='" + eventId + '\'' +
                    '}';
        }
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
