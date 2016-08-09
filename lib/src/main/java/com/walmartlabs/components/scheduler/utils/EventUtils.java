package com.walmartlabs.components.scheduler.utils;

import com.walmart.marketplace.messages.v1_bigben.EventRequest;
import com.walmart.marketplace.messages.v1_bigben.EventResponse;
import com.walmartlabs.components.scheduler.entities.Event;

import static com.walmart.marketplace.messages.v1_bigben.EventResponse.Status.fromValue;

/**
 * Created by smalik3 on 8/8/16
 */
public class EventUtils {

    public static EventResponse fromRequest(EventRequest eventRequest) {
        final EventResponse eventResponse = new EventResponse();
        eventResponse.setTenant(eventRequest.getTenant());
        eventResponse.setEventTime(eventRequest.getEventTime());
        eventResponse.setId(eventRequest.getId());
        eventResponse.setPayload(eventRequest.getPayload());
        eventResponse.setMode(eventRequest.getMode());
        return eventResponse;
    }

    public static EventResponse toResponse(Event e) {
        EventResponse eventResponse = new EventResponse();
        eventResponse.setId(e.getXrefId());
        eventResponse.setEventId(e.id().getEventId());
        eventResponse.setTriggeredAt(e.getProcessedAt() == null ? null : e.getProcessedAt().toString());
        eventResponse.setTenant(e.getTenant());
        eventResponse.setEventTime(e.id().getEventTime().toString());
        eventResponse.setPayload(e.getPayload());
        eventResponse.setStatus(fromValue(e.getStatus()));
        return eventResponse;
    }

}
