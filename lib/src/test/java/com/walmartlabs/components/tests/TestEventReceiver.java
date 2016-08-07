package com.walmartlabs.components.tests;

import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.entities.*;
import com.walmartlabs.components.scheduler.entities.EventDO.EventKey;
import com.walmartlabs.components.scheduler.entities.EventLookupDO.EventLookupKey;
import com.walmartlabs.components.scheduler.input.EventReceiver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;

import static com.walmartlabs.components.scheduler.entities.EventRequest.Mode.ADD;
import static com.walmartlabs.components.scheduler.entities.Status.*;
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
        setProperty("com.walmart.platform.config.runOnEnv", "prod");
        setProperty("event.shard.size", "10");
        setProperty("hazelcast.slow.operation.detector.stacktrace.logging.enabled", "true");
        setProperty("com.walmart.platform.config.appName", "gmp-solr-consumer");
        setProperty("hz.config", "hz_local");
        setProperty("ccmProps", "bigbenProps");
    }

    @Autowired
    private EventReceiver eventReceiver;

    @Autowired
    private DataManager<EventLookupKey, EventLookup> lookupManager;

    @Autowired
    private DataManager<EventKey, Event> eventDataManager;

    @Autowired
    private DataManager<ZonedDateTime, Bucket> bucketDataManager;

    @Test
    public void testEventReceiver() throws Exception {
        final EventRequest eventRequest = new EventRequest();
        eventRequest.setId(randomUUID().toString());
        eventRequest.setTenant("TEMP");
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
        compareLookup(eventRequest, updatedEventResponse, eventLookup);
        compareEvent(eventRequest, eventLookup);
        compareRequestAndResponse(eventRequest, updatedEventResponse, UPDATED);

        // update event time:
        eventRequest.setEventTime(parse(eventRequest.getEventTime()).plusSeconds(1).toString());
        eventRequest.setPayload("payload345");
        final EventResponse updatedEventResponse1 = eventReceiver.addEvent(eventRequest).get();
        assertEquals(updatedEventResponse1.getStatus(), UPDATED.name());
        final EventLookup eventLookup1 = lookupManager.get(eventLookupKey);

        assertEquals(eventLookup.id().getXrefId(), eventLookup1.id().getXrefId());
        assertEquals(eventLookup.id().getTenant(), eventLookup1.id().getTenant());
        assertNotEquals(eventLookup.getEventTime(), eventLookup1.getEventTime());
        assertNotEquals(eventLookup.getEventId(), eventLookup1.getEventId());
        compareLookup(eventRequest, updatedEventResponse1, eventLookup1);
        compareEvent(eventRequest, eventLookup1);

        //delete:
        final EventResponse eventResponse1 = eventReceiver.removeEvent(eventRequest.getId(), eventRequest.getTenant()).get();
        assertEquals(eventResponse1.getStatus(), DELETED.name());
        assertEquals(eventResponse1.getId(), eventRequest.getId());
        assertEquals(eventResponse1.getTenant(), eventRequest.getTenant());
        assertNull(lookupManager.get(eventLookupKey));
        assertNull(eventDataManager.get(EventKey.of(eventLookup.getBucketId(), 0, eventLookup.getEventTime(), eventLookup.getEventId())));
    }

    private void compareEvent(EventRequest eventRequest, EventLookup eventLookup) {
        final Event event = eventDataManager.get(EventKey.of(eventLookup.getBucketId(), 0, eventLookup.getEventTime(), eventLookup.getEventId()));
        assertNotNull(event);
        assertEquals(event.getPayload(), eventRequest.getPayload());
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
        assertEquals(eventResponse.getStatus(), status.name());
        assertEquals(eventResponse.getTenant(), eventRequest.getTenant());
        assertEquals(eventResponse.getMode(), eventRequest.getMode());
        assertEquals(eventResponse.getEventTime(), eventRequest.getEventTime());
        assertEquals(eventResponse.getId(), eventRequest.getId());
    }

}
