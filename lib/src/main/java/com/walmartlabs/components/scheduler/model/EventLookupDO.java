package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.KeyMapping;
import com.walmart.gmp.ingestion.platform.framework.data.core.MutableEntity;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.PartitionKey;

import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static java.lang.String.format;

/**
 * Created by smalik3 on 3/29/16
 */
@Entity(table = "event_lookup")
@KeyMapping(keyClass = EventLookupDO.EventLookupKey.class, entityClass = EventLookupEntity.class, version = V1)
public class EventLookupDO implements EventLookupEntity, MutableEntity<EventLookupDO.EventLookupKey> {

    @EmbeddedId
    private EventLookupKey id;

    @Column
    private long offset;

    @Column
    private int shard;

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public void setOffset(long offset) {
        this.offset = offset;
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
    public void id(EventLookupKey eventLookupKey) {
        this.id = eventLookupKey;
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
        return id;
    }

    public EventLookupKey getId() {
        return id;
    }

    public void setId(EventLookupKey id) {
        this.id = id;
    }

    public static class EventLookupKey {
        @PartitionKey
        @Column(name = "event_time")
        private long eventTime;

        @PartitionKey(2)
        @Column(name = "event_id")
        private String eventId;

        public static EventLookupKey of(long eventTime, String eventId) {
            final EventLookupKey eventLookupKey = new EventLookupKey();
            eventLookupKey.eventTime = eventTime;
            eventLookupKey.eventId = eventId;
            return eventLookupKey;
        }

        public long getEventTime() {
            return eventTime;
        }

        public void setEventTime(long eventTime) {
            this.eventTime = eventTime;
        }

        public String getEventId() {
            return eventId;
        }

        public void setEventId(String eventId) {
            this.eventId = eventId;
        }

        @Override
        public String toString() {
            return format("EventLookKey[%d/%s]", eventTime, eventId);
        }
    }

}
