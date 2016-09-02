package com.walmartlabs.components.scheduler.entities.codecs;

import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.Constants;
import com.walmart.marketplace.messages.v1_bigben.EventResponse;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by hpatel2 on 9/1/16
 */
public class EventResponseToPayloadSerializer implements Serializer<String> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, String data) {
        if (data == null)
            return new byte[0];
        final EventResponse eventResponse;
        try {
            eventResponse = Constants.OBJECT_MAPPER.readValue(data, EventResponse.class);
            return eventResponse.getPayload().getBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
