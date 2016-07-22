package com.walmartlabs.components.scheduler.entities;

import static com.walmartlabs.components.scheduler.entities.EventRequest.Mode.ADD;

/**
 * Created by smalik3 on 6/22/16
 */
public class EventRequest {

    private String eventTime;
    private String id;
    private String payload;
    private String tenant;
    private Mode mode = ADD;

    public enum Mode {ADD, REMOVE}

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

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    @Override
    public String toString() {
        return "EventRequest{" +
                "eventTime='" + eventTime + '\'' +
                ", id='" + id + '\'' +
                ", payload='" + payload + '\'' +
                ", tenant='" + tenant + '\'' +
                ", mode=" + mode +
                '}';
    }
}
