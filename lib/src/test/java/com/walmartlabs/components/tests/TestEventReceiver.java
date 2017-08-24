package com.walmartlabs.components.tests;

import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.marketplace.messages.v1_bigben.BulkEventRequest;
import com.walmart.marketplace.messages.v1_bigben.EventRequest;
import com.walmart.marketplace.messages.v1_bigben.EventResponse;
import com.walmart.marketplace.messages.v1_bigben.EventResponse.Status;
import com.walmart.services.common.util.JsonUtil;
import com.walmartlabs.components.scheduler.entities.Bucket;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventDO.EventKey;
import com.walmartlabs.components.scheduler.entities.EventLookup;
import com.walmartlabs.components.scheduler.entities.EventLookupDO;
import com.walmartlabs.components.scheduler.entities.EventLookupDO.EventLookupKey;
import com.walmartlabs.components.scheduler.input.BulkMessageProcessor;
import com.walmartlabs.components.scheduler.input.EventReceiver;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.concurrent.CountDownLatch;

import static com.walmart.marketplace.messages.v1_bigben.EventRequest.Mode.ADD;
import static com.walmart.marketplace.messages.v1_bigben.EventResponse.Status.*;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.*;
import static java.lang.System.setProperty;
import static java.time.ZonedDateTime.parse;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.*;

/**
 * Created by smalik3 on 7/21/16
 */
@ContextConfiguration(locations = {"/test-scheduler.xml"})
public class TestEventReceiver extends AbstractTestNGSpringContextTests {

    static {
        setProperty("dm.entity.packages.scan", "com.walmartlabs.components.scheduler.entities");
        setProperty("com.walmart.platform.config.runOnEnv", "stg0");
        setProperty("event.shard.size", "10");
        setProperty("hazelcast.slow.operation.detector.stacktrace.logging.enabled", "true");
        setProperty("com.walmart.platform.config.appName", "gmp-solr-consumer");
        setProperty("hz.config", "hz_local");
        setProperty("ccmProps", "bigben_demo");
    }

    @Autowired
    private EventReceiver eventReceiver;

    @Autowired
    private BulkMessageProcessor bulkMessageProcessor;

    @Autowired
    @Qualifier("bigbenDataManager")
    private DataManager<EventLookupKey, EventLookup> lookupManager;

    @Autowired
    @Qualifier("bigbenDataManager")
    private DataManager<EventKey, Event> eventDataManager;

    @Autowired
    @Qualifier("bigbenDataManager")
    private DataManager<ZonedDateTime, Bucket> bucketDataManager;

    @Test()
    public void testEventReceiver() throws Exception {
        final EventRequest eventRequest = new EventRequest();
        eventRequest.setId(randomUUID().toString());
        eventRequest.setTenant("$$TEST$$");
        eventRequest.setPayload("payload123");
        eventRequest.setMode(ADD);
        eventRequest.setEventTime(nowUTC().plusMinutes(2).toString());
        final EventResponse eventResponse = eventReceiver.addEvent(eventRequest).get();
        compareRequestAndResponse(eventRequest, eventResponse, ACCEPTED);

        final EventLookupKey eventLookupKey = new EventLookupKey(eventRequest.getId(), eventRequest.getTenant());
        final EventLookup eventLookup = lookupManager.get(eventLookupKey);

        compareLookup(eventRequest, eventResponse, eventLookup);
        compareEvent(eventRequest, eventLookup);

        final Bucket bucket = bucketDataManager.get(eventLookup.getBucketId());
        assertTrue(bucket.getCount() > 0);
        assertNull(bucket.getStatus());

        // update:
        eventRequest.setPayload("payload234");
        final EventResponse updatedEventResponse = eventReceiver.addEvent(eventRequest).get();
        final EventLookup eventLookup0 = lookupManager.get(eventLookupKey);
        compareLookup(eventRequest, updatedEventResponse, eventLookup0);
        compareEvent(eventRequest, eventLookup0);
        compareRequestAndResponse(eventRequest, updatedEventResponse, UPDATED);

        // update event time:
        eventRequest.setEventTime(parse(eventRequest.getEventTime()).plusSeconds(1).toString());
        eventRequest.setPayload("payload345");
        final EventResponse updatedEventResponse1 = eventReceiver.addEvent(eventRequest).get();
        assertEquals(updatedEventResponse1.getStatus(), UPDATED);
        final EventLookup eventLookup1 = lookupManager.get(eventLookupKey);

        assertEquals(eventLookup.id().getXrefId(), eventLookup1.id().getXrefId());
        assertEquals(eventLookup.id().getTenant(), eventLookup1.id().getTenant());
        assertNotEquals(eventLookup.getEventTime(), eventLookup1.getEventTime());
        assertNotEquals(eventLookup.getEventId(), eventLookup1.getEventId());
        compareLookup(eventRequest, updatedEventResponse1, eventLookup1);
        compareEvent(eventRequest, eventLookup1);

        //delete:
        final EventResponse eventResponse1 = eventReceiver.removeEvent(eventRequest.getId(), eventRequest.getTenant()).get();
        assertEquals(eventResponse1.getStatus(), DELETED);
        assertEquals(eventResponse1.getId(), eventRequest.getId());
        assertEquals(eventResponse1.getTenant(), eventRequest.getTenant());
        assertNull(lookupManager.get(eventLookupKey));
        assertNull(eventDataManager.get(EventKey.of(eventLookup.getBucketId(), 0, eventLookup.getEventTime(), eventLookup.getEventId())));

        new CountDownLatch(1).await();
    }



    @Test
    public void testBulkEventReceiver() throws Exception {
        final BulkEventRequest bulkEventRequest = new BulkEventRequest();
        for(int i = 0; i < 2; ++i) {
            EventRequest eventRequest = new EventRequest();
            eventRequest.setId(randomUUID().toString());
            eventRequest.setTenant("$$TEST$$");
            eventRequest.setPayload("payload123-" + i);
            eventRequest.setMode(ADD);
            eventRequest.setEventTime(nowUTC().plusMinutes(2).toString());
            bulkEventRequest.getEventRequests().add(eventRequest);
            bulkEventRequest.setSenderId("vudu");
            bulkEventRequest.setId("vudu-test-2");
        }
        String bulkrequest = JsonUtil.convertToString(bulkEventRequest);
        final EventResponse eventResponse;
        final ConsumerRecord<String, String> strConsumerRecord = bulkMessageProcessor.apply("dummyTopic", new ConsumerRecord<String, String>("dummyTopic", 1, 100L, "mig1", bulkrequest)).get();


        final EventLookupDO.EventLookupKey eventLookupKey = new EventLookupDO.EventLookupKey(bulkEventRequest.getId(), bulkEventRequest.getSenderId());
        final EventLookup eventLookup = lookupManager.get(eventLookupKey);
        String payload = eventLookup.getPayload();
        Assert.assertEquals(StringUtils.contains(payload,bulkEventRequest.getEventRequests().get(0).getId()), true);
    }

    private void compareEvent(EventRequest eventRequest, EventLookup eventLookup) {
        final Event event = eventDataManager.get(EventKey.of(eventLookup.getBucketId(), 0, eventLookup.getEventTime(), eventLookup.getEventId()));
        assertNotNull(event);
        assertEquals(eventLookup.getPayload(), eventRequest.getPayload());
        assertEquals(event.getStatus(), UN_PROCESSED.name());
        assertEquals(event.getTenant(), eventRequest.getTenant());
        assertEquals(event.getXrefId(), eventRequest.getId());
        assertNull(event.getError());
        assertEquals(event.getProcessedAt(), null);
    }

    private void compareLookup(EventRequest eventRequest, EventResponse eventResponse, EventLookup eventLookup) {
        assertEquals(eventRequest.getEventTime(), eventLookup.getEventTime().toString());
        assertEquals(eventLookup.getEventId(), eventResponse.getEventId());
        assertEquals(eventLookup.getBucketId(), utc(bucketize(parse(eventRequest.getEventTime()).toInstant().toEpochMilli(), 1)));
    }

    private void compareRequestAndResponse(EventRequest eventRequest, EventResponse eventResponse, Status status) {
        assertNull(eventResponse.getErrors());
        assertTrue(eventResponse.getEventId() != null);
        assertEquals(eventResponse.getStatus(), status);
        assertEquals(eventResponse.getTenant(), eventRequest.getTenant());
        assertEquals(eventResponse.getMode(), eventRequest.getMode());
        assertEquals(eventResponse.getEventTime(), eventRequest.getEventTime());
        assertEquals(eventResponse.getId(), eventRequest.getId());
    }

}
