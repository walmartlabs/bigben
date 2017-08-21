package com.walmartlabs.components.scheduler.input;

import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.marketplace.messages.v1_bigben.BulkEventRequest;
import com.walmart.marketplace.messages.v1_bigben.EventResponse;
import com.walmartlabs.components.scheduler.entities.EventLookup;
import com.walmartlabs.components.scheduler.entities.EventLookupDO;
import com.walmartlabs.components.scheduler.utils.TimeUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Throwables.getRootCause;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.services.common.util.JsonUtil.convertToObject;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.bucketize;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.utc;
import static java.time.ZonedDateTime.parse;


/**
 * Created by cshah on 7/7/17
 * <p>
 * This is to consume and update bulk events.
 */
@Component
public class BulkMessageProcessor extends MessageProcessor implements InitializingBean {

    private static final Logger L = Logger.getLogger(BulkMessageProcessor.class);

    @Autowired
    @Qualifier("bigbenDataManager")
    private DataManager<EventLookupDO.EventLookupKey, EventLookup> lookupDataManager;

    Exception exception = new Exception("immediate failed future as one of the listnable future failed to process");

    private final ListenableFuture<ConsumerRecord<String, String>> SUCCESS = immediateFuture(null);
    private final ListenableFuture<ConsumerRecord<String, String>> FAIL = immediateFailedFuture(exception);
    private final ListenableFuture<BulkEventRequest> SUCCESS_EVENT = immediateFuture(null);
    ListenableFuture<BulkEventRequest> FAIL_EVENT = immediateFailedFuture(exception);

    private final String IDMERG = "$$||$$";
    private int scanInterval;
    private int lapseOffset;

    @Autowired
    private EventReceiver eventReceiver;

    @Override
    public void afterPropertiesSet() throws Exception {
        scanInterval = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
        lapseOffset = PROPS.getInteger("event.lapse.offset.minutes", 0);
        L.info("using event lapseOffset: " + lapseOffset + " minutes");
    }


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
        final long eventTimeMillis = System.currentTimeMillis();
        final ZonedDateTime bucketId = utc(bucketize(eventTimeMillis, scanInterval));
        final ListenableFuture<BulkEventRequest> bulkEventRequestListenableFuture = deleteEventLookup(bulkEventRequest);
        return transformAsync(bulkEventRequestListenableFuture, $ -> {
            if (bulkEventRequest != null && bulkEventRequest.getEvents() != null)
                bulkEventRequest.getEvents().forEach(s -> allEventRequestId.add(s.getId() + IDMERG + s.getTenant()));
            final EventLookupDO.EventLookupKey eventLookupKey = new EventLookupDO.EventLookupKey(bulkEventRequest.getId(), bulkEventRequest.getSenderId());
            EventLookup eventLookupEntity = DataManager.entity(EventLookup.class, eventLookupKey);
            eventLookupEntity.setPayload(String.join("$$,$$", allEventRequestId));
            eventLookupEntity.setCreatedAt(TimeUtils.nowUTC());
            eventLookupEntity.setModifiedAt(TimeUtils.nowUTC());
            eventLookupEntity.setEventId(bulkEventRequest.getMessageId());
            eventLookupEntity.setShard(0);
            eventLookupEntity.setBucketId(bucketId);
            return lookupDataManager.saveAsync(eventLookupEntity);
        });
    }

    private ListenableFuture<BulkEventRequest> deleteEventLookup(BulkEventRequest bulkEventRequest) {
        if (bulkEventRequest != null && bulkEventRequest.getEvents() != null) {
            final EventLookupDO.EventLookupKey eventLookupKey = new EventLookupDO.EventLookupKey(bulkEventRequest.getId(), bulkEventRequest.getSenderId());
            EventLookup eventLookupEntity = lookupDataManager.get(eventLookupKey);
            final List<ListenableFuture<EventResponse>> allListenableFuture = new ArrayList<>();
            if (eventLookupEntity != null) {
                String payload = eventLookupEntity.getPayload();
                if (payload != null) {
                    final List<String> listofEventToDelete = Arrays.asList(payload.split("$$,$$"));
                    for (String eventId : listofEventToDelete) {
                        final String[] split = eventId.split(IDMERG);
                        if (split != null && split.length > 1) {
                            final ListenableFuture<EventResponse> eventResponseListenableFuture = eventReceiver.removeEvent(split[0], split[1]);
                            allListenableFuture.add(eventResponseListenableFuture);
                        } else L.error("Incorrect Id found. Fix for this events : " + eventLookupKey.getXrefId());
                    }

                }
            }
            return transformAsync(allAsList(allListenableFuture), l -> SUCCESS_EVENT);
        }
        return SUCCESS_EVENT;
    }
}
