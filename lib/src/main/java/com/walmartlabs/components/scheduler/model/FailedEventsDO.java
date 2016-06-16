package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.KeyMapping;
import com.walmart.gmp.ingestion.platform.framework.data.core.MutableEntity;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.PartitionKey;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static com.walmart.services.common.util.JsonUtil.convertToObject;
import static com.walmart.services.common.util.JsonUtil.convertToString;

/**
 * Created by smalik3 on 6/14/16
 */
@Entity(table = "failed_events")
@KeyMapping(keyClass = String.class, entityClass = FailedEvents.class, version = V1)
public class FailedEventsDO implements FailedEvents, MutableEntity<String> {

    @PartitionKey
    private String bucketId;

    @Column
    private String serializedFailedEvents;

    @Override
    public void id(String s) {
        bucketId = s;
    }

    @Override
    public void key(Object key) {
        bucketId = key.toString();
    }

    @Override
    public void setFailedEvents(List<EventKey> events) {
        try {
            serializedFailedEvents = convertToString(events);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<EventKey> getFailedEvents() {
        try {
            return serializedFailedEvents == null ? new ArrayList<>() : convertToObject(serializedFailedEvents,
                    new TypeReference<List<EventKey>>() {
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getSerializedFailedEvents() {
        return serializedFailedEvents;
    }

    public void setSerializedFailedEvents(String serializedFailedEvents) {
        this.serializedFailedEvents = serializedFailedEvents;
    }

    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(String bucketId) {
        this.bucketId = bucketId;
    }

    @Override
    public String id() {
        return bucketId;
    }

    @Override
    public Object key() {
        return bucketId;
    }
}
