package com.walmartlabs.components.scheduler.input;

import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.TopicMessageProcessor;
import com.walmartlabs.components.scheduler.entities.EventRequest;
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.services.common.util.JsonUtil.convertToObject;
import static com.walmartlabs.components.scheduler.entities.EventRequest.Mode.REMOVE;
import static com.walmartlabs.components.scheduler.entities.Status.ERROR;
import static com.walmartlabs.components.scheduler.entities.Status.REJECTED;
import static com.walmartlabs.components.scheduler.input.EventReceiver.toEvent;

/**
 * Created by smalik3 on 6/22/16
 */
public class MessageProcessor implements TopicMessageProcessor {

    private static final Logger L = Logger.getLogger(MessageProcessor.class);

    @Autowired
    private EventReceiver eventReceiver;

    @Autowired
    private ProcessorRegistry processorRegistry;

    private final ListenableFuture<ConsumerRecord<String, String>> SUCCESS = immediateFuture(null);
    private final ListenableFuture<ConsumerRecord<String, String>> FAIL = immediateFuture(null);

    @Override
    public ListenableFuture<ConsumerRecord<String, String>> apply(String topic, ConsumerRecord<String, String> record) {
        try {
            final EventRequest eventRequest = convertToObject(record.value(), EventRequest.class);
            if (eventRequest.getMode() == REMOVE) {
                return catchingAsync(transformAsync(eventReceiver.removeEvent(eventRequest.getId(), eventRequest.getTenant()), e -> {
                    if (e == null) {
                        L.warn("null response received: " + record);
                        return FAIL;
                    } else if (ERROR.name().equals(e.getStatus())) {
                        L.error("failed to add event: " + e);
                        return FAIL;
                    }
                    return SUCCESS;
                }), Exception.class, ex -> {
                    L.error("error in removing event: " + record, getRootCause(ex));
                    return FAIL;
                });
            } else {
                return catchingAsync(transformAsync(eventReceiver.addEvent(eventRequest), e -> {
                    if (e == null) {
                        L.warn("null response received: " + record);
                        return FAIL;
                    } else if (ERROR.name().equals(e.getStatus())) {
                        L.error("failed to add event: " + e);
                        return FAIL;
                    }
                    if (REJECTED.name().equals(e.getStatus())) {
                        L.warn("event was rejected, failure status: " + e.getStatus());
                        return transformAsync(processorRegistry.process(toEvent(e)), $ -> SUCCESS);
                    }
                    return SUCCESS;
                }), Exception.class, ex -> {
                    L.error("error in removing event: " + record, getRootCause(ex));
                    return FAIL;
                });
            }
        } catch (Exception e) {
            L.error("could not process record: " + record);
            return FAIL;
        }
    }
}
