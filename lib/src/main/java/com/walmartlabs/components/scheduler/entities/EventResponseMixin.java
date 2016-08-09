package com.walmartlabs.components.scheduler.entities;

import com.walmart.marketplace.messages.v1_bigben.EventResponse;

/**
 * Created by smalik3 on 7/15/16
 */
public interface EventResponseMixin {

    EventResponse getEventResponse();

    void setEventResponse(EventResponse eventResponse);

}
