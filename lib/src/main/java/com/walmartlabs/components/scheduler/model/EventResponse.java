package com.walmartlabs.components.scheduler.model;

import com.walmart.platform.kernel.exception.error.Error;

import java.util.Collections;
import java.util.List;

import static com.walmart.platform.kernel.exception.error.ErrorCategory.SYSTEM;
import static com.walmart.platform.kernel.exception.error.ErrorSeverity.ERROR;
import static java.lang.System.currentTimeMillis;

/**
 * Created by smalik3 on 6/22/16
 */
public class EventResponse extends EventRequest {

    private List<Error> errors;
    private long processedUtc;
    private String xrefId;

    public List<Error> getErrors() {
        return errors;
    }

    public void setErrors(List<Error> errors) {
        this.errors = errors;
    }

    public long getProcessedUtc() {
        return processedUtc;
    }

    public void setProcessedUtc(long processedUtc) {
        this.processedUtc = processedUtc;
    }

    public String getXrefId() {
        return xrefId;
    }

    public void setXrefId(String xrefId) {
        this.xrefId = xrefId;
    }

    public static EventResponse fromRequest(EventRequest eventRequest) {
        final EventResponse eventResponse = new EventResponse();
        eventResponse.setTenant(eventRequest.getTenant());
        eventResponse.setUtc(eventRequest.getUtc());
        eventResponse.setId(eventRequest.getId());
        eventResponse.setPayload(eventRequest.getPayload());
        return eventResponse;
    }

    @Override
    public String toString() {
        return "EventResponse{" +
                "errors=" + errors +
                ", processedUtc=" + processedUtc +
                ", xrefId='" + xrefId + '\'' +
                "} " + super.toString();
    }

    public static EventResponse toResponse(Event e) {
        EventResponse eventResponse = new EventResponse();
        eventResponse.setId(e.getXrefId());
        eventResponse.setXrefId(e.id().getEventId());
        eventResponse.setProcessedUtc(currentTimeMillis());
        eventResponse.setTenant(e.getTenant());
        eventResponse.setUtc(e.id().getEventTime());
        eventResponse.setPayload(e.getPayload());
        final Error error = new Error("000", "unknown", e.getError(), e.getError(), ERROR, SYSTEM);
        final List<Error> errors = Collections.singletonList(error);
        eventResponse.setErrors(errors);
        return eventResponse;
    }
}
