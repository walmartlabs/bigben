package com.walmartlabs.components.scheduler.services;

/**
 * Created by smalik3 on 6/27/16
 */
public class BulkEventGeneration {

    private String startTime;
    private long period;
    private int numEvents;
    private String tenantId;

    public BulkEventGeneration(String startTime, long period, int numEvents, String tenantId) {
        this.startTime = startTime;
        this.period = period;
        this.numEvents = numEvents;
        this.tenantId = tenantId;
    }

    public BulkEventGeneration() {
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public int getNumEvents() {
        return numEvents;
    }

    public void setNumEvents(int numEvents) {
        this.numEvents = numEvents;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public String toString() {
        return "BulkEventGeneration{" +
                "startTime='" + startTime + '\'' +
                ", period=" + period +
                ", numEvents=" + numEvents +
                ", tenantId='" + tenantId + '\'' +
                '}';
    }
}
