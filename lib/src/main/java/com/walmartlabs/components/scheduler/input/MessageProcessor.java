package com.walmartlabs.components.scheduler.input;

import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.TopicMessageProcessor;
import com.walmartlabs.components.scheduler.entities.EventRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.walmart.services.common.util.JsonUtil.convertToObject;

/**
 * Created by smalik3 on 6/22/16
 */
public class MessageProcessor implements TopicMessageProcessor {

    private static final Logger L = Logger.getLogger(MessageProcessor.class);

    @Autowired
    private EventReceiver eventReceiver;

    private final ListenableFuture<ConsumerRecord<String, String>> SUCCESS = immediateFuture(null);

    @Override
    public ListenableFuture<ConsumerRecord<String, String>> apply(String topic, ConsumerRecord<String, String> record) {
        try {
            final EventRequest eventRequest = convertToObject(record.value(), EventRequest.class);
            return transformAsync(eventReceiver.addEvent(eventRequest), e -> SUCCESS);
        } catch (Exception e) {
            L.error("could not process record: " + record);
            return SUCCESS;
        }
    }
}
