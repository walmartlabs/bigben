package com.walmartlabs.components.scheduler.input;

import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.processors.TopicMessageProcessor;
import com.walmart.marketplace.messages.v1_bigben.BulkEventRequest;
import com.walmart.marketplace.messages.v1_bigben.EventRequest;
import com.walmartlabs.components.scheduler.entities.EventLookup;
import com.walmartlabs.components.scheduler.entities.EventLookupDO;
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import com.walmartlabs.components.scheduler.utils.TimeUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Throwables.getRootCause;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.marketplace.messages.v1_bigben.EventRequest.Mode.REMOVE;
import static com.walmart.marketplace.messages.v1_bigben.EventResponse.Status.*;
import static com.walmart.services.common.util.JsonUtil.convertToObject;
import static com.walmartlabs.components.scheduler.input.EventReceiver.toEvent;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.nowUTC;
import static java.lang.String.format;
import static java.time.ZonedDateTime.parse;

/**
 * Created by cshah on 7/7/17
 * <p>
 * This is to consume and update bulk events.
 */
public class BulkMessageProcessor extends MessageProcessor {

    private static final Logger L = Logger.getLogger(BulkMessageProcessor.class);

    @Autowired
    @Qualifier("bigbenDataManager")
    private DataManager<EventLookupDO.EventLookupKey, EventLookup> lookupDataManager;

    private final ListenableFuture<ConsumerRecord<String, String>> SUCCESS = immediateFuture(null);
    private final ListenableFuture<ConsumerRecord<String, String>> FAIL = immediateFuture(null);

    @Override
    public ListenableFuture<ConsumerRecord<String, String>> apply(String topic, ConsumerRecord<String, String> record) {
        try {

            final BulkEventRequest bulkEventRequest = convertToObject(record.value(), BulkEventRequest.class);
            return transformAsync(updateEventLookup(bulkEventRequest), $ -> {
                final List<ListenableFuture<ConsumerRecord<String, String>>> allListenableFuture = new ArrayList<>();
                bulkEventRequest.getEvents().forEach(r -> {
                    ListenableFuture<ConsumerRecord<String, String>> singleListenableFuture = super.updateEvent(record, r);
                    allListenableFuture.add(singleListenableFuture);
                });
                return transformAsync(allAsList(allListenableFuture), l -> SUCCESS);
            });
        } catch (Exception e) {
            L.error("could not process record: " + record, getRootCause(e));
            return FAIL;
        }
    }

    private ListenableFuture<EventLookup> updateEventLookup(BulkEventRequest bulkEventRequest) {
        List<String> allEventRequestId = new ArrayList<>();
        if (bulkEventRequest != null && bulkEventRequest.getEvents() != null)
            bulkEventRequest.getEvents().forEach(s -> allEventRequestId.add(s.getId()));
        final EventLookupDO.EventLookupKey eventLookupKey = new EventLookupDO.EventLookupKey(bulkEventRequest.getId(), bulkEventRequest.getSenderId());
        EventLookup eventLookupEntity = DataManager.entity(EventLookup.class, eventLookupKey);
        eventLookupEntity.setPayload(String.join(", ", allEventRequestId));
        eventLookupEntity.setCreatedAt(TimeUtils.nowUTC());
        eventLookupEntity.setModifiedAt(TimeUtils.nowUTC());
        eventLookupEntity.setEventId(bulkEventRequest.getMessageId());
        return lookupDataManager.saveAsync(eventLookupEntity);

    }
}
