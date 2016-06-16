package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.KeyMapping;
import com.walmart.gmp.ingestion.platform.framework.data.core.MutableEntity;
import info.archinnov.achilles.annotations.*;

import java.io.Serializable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static java.lang.Long.compare;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by smalik3 on 3/8/16
 */
@Entity(table = "event_schedule")
@KeyMapping(keyClass = EventDO.EventKey.class, entityClass = Event.class, version = V1)
public class EventDO implements Serializable, Delayed, Event, MutableEntity<EventDO.EventKey> {

    @EmbeddedId
    private EventKey eventKey;

    //@Index
    @Column
    private String state;

    //@Index
    @Column
    private String error;

    @Override
    public String getState() {
        return state;
    }

    @Override
    public void setState(String state) {
        this.state = state;
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

    @Override
    public long getDelay(TimeUnit unit) {
        return 0L;
    }

    @Override
    public int compareTo(Delayed o) {
        return compare(getDelay(SECONDS), o.getDelay(SECONDS));
    }

    public static class EventKey {
        @PartitionKey
        @Column(name = "offset_time")
        private long offsetTime;

        @PartitionKey(2)
        @Column(name = "shard")
        private int shard;

        @ClusteringColumn
        @Column(name = "event_time")
        private long eventTime;

        @ClusteringColumn(2)
        @Column(name = "event_id")
        private String eventId;

        public static EventKey of(long offsetTime, int shard, long eventTime, String eventId) {
            final EventKey eventKey = new EventKey();
            eventKey.offsetTime = offsetTime;
            eventKey.eventTime = eventTime;
            eventKey.eventId = eventId;
            eventKey.shard = shard;
            return eventKey;
        }

        public long getOffsetTime() {
            return offsetTime;
        }

        public void setOffsetTime(long offsetTime) {
            this.offsetTime = offsetTime;
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
            return format("Event[%d/%d/%d/%s]", offsetTime, shard, eventTime, eventId);
        }
    }

    @Override
    public String toString() {
        return "EventDO{" +
                "eventKey=" + eventKey +
                ", state='" + state + '\'' +
                ", error='" + error + '\'' +
                '}';
    }
}
