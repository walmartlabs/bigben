package com.walmartlabs.components.scheduler.entities.codecs;

import com.walmart.marketplace.messages.v1_bigben.EventResponse;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by hpatel2 on 9/1/16
 */
public class EventResponseToPayloadSerializer implements Serializer<EventResponse> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, EventResponse data) {
        if (data == null || data.getPayload() == null)
            return new byte[0];
        return data.getPayload().getBytes();
    }

    @Override
    public void close() {

    }
}
