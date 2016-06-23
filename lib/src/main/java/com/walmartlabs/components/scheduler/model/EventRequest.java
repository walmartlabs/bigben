package com.walmartlabs.components.scheduler.model;

/**
 * Created by smalik3 on 6/22/16
 */
public class EventRequest {

    private long utc;
    private String id;
    private String payload;
    private String tenant;

    public long getUtc() {
        return utc;
    }

    public void setUtc(long utc) {
        this.utc = utc;
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
                "utc=" + utc +
                ", id='" + id + '\'' +
                ", payload='" + payload + '\'' +
                ", tenant='" + tenant + '\'' +
                '}';
    }
}
