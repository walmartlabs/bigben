package com.walmartlabs.components.scheduler.entities;

import com.walmart.services.common.util.JsonUtil;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * Created by smalik3 on 6/22/16
 */
public class EventRequest {

    private String eventTime;
    private String id;
    private String payload;
    private String tenant;

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    @Override
    public String toString() {
        return "EventRequest{" +
                "eventTime='" + eventTime + '\'' +
                ", id='" + id + '\'' +
                ", payload='" + payload + '\'' +
                ", tenant='" + tenant + '\'' +
                '}';
    }

    public static void main(String[] args) throws IOException {
        EventRequest eventRequest = new EventRequest();
        eventRequest.setId(UUID.randomUUID().toString());
        eventRequest.setPayload("some payload");
        eventRequest.setTenant("PROMO_STG");
        eventRequest.setEventTime("2016-07-13T21:15Z");
        System.out.println(JsonUtil.convertToString(eventRequest));
    }
}
