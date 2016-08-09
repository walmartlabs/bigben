package com.walmartlabs.components.scheduler.processors;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.TopicMessageProcessor;
import com.walmart.marketplace.messages.v1_bigben.EventResponse;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventLookup;
import com.walmartlabs.components.scheduler.entities.EventLookupDO.EventLookupKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.raw;
import static com.walmart.marketplace.messages.v1_bigben.EventResponse.Status.PROCESSED;
import static com.walmart.marketplace.messages.v1_bigben.EventResponse.Status.PROCESSING;
import static com.walmart.services.common.util.JsonUtil.convertToObject;
import static com.walmartlabs.components.scheduler.input.EventReceiver.toEvent;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.nowUTC;

/**
 * Created by smalik3 on 8/7/16
 */
public class AsyncEventEnrichProcessor extends EventEnrichProcessor implements EventProcessor<Event>, TopicMessageProcessor {

    private static final Logger L = Logger.getLogger(EventEnrichProcessor.class);

    public static final String _EVENT_ENRICHMENT_ = "_EVENT_ENRICHMENT_";

    public AsyncEventEnrichProcessor(ProcessorRegistry processorRegistry, DataManager<EventLookupKey, EventLookup> lookupDataManager) {
        super(processorRegistry, lookupDataManager);
    }

    @Override
    public ListenableFuture<Event> process(Event event) {
        final Event e = raw(entity(Event.class, event.id()));
        e.setXrefId(event.getXrefId());
        e.setTenant(_EVENT_ENRICHMENT_);
        e.setPayload(event.getTenant());
        e.setError(event.getError());
        e.setStatus(event.getStatus());
        return transform(super.process(e), (Function<Event, Event>) re -> {
            L.debug("event posted successfully: " + event.id());
            event.setStatus(re.getStatus());
            if (re.getError() != null)
                event.setError(re.getError());
            return event;
        });
    }

    private final ListenableFuture<ConsumerRecord<String, String>> SUCCESS = immediateFuture(null);
    private final ListenableFuture<ConsumerRecord<String, String>> FAIL = immediateFuture(null);

    @Override
    public ListenableFuture<ConsumerRecord<String, String>> apply(String topic, ConsumerRecord<String, String> record) {
        try {
            final EventResponse eventResponse = convertToObject(record.value(), EventResponse.class);
            eventResponse.setTriggeredAt(nowUTC().toString());
            eventResponse.setStatus(PROCESSING);
            final Event event = toEvent(eventResponse);
            event.setTenant(event.getPayload());
            return transformAsync(super.process(event), e -> {
                if (!PROCESSED.name().equals(e.getStatus()))
                    return FAIL;
                else return SUCCESS;
            });
        } catch (Exception e) {
            L.error("could not process record: " + record);
            return FAIL;
        }
    }
}
