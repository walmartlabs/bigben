package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;

import java.util.List;

/**
 * Created by smalik3 on 6/14/16
 */
public interface FailedEvents extends Entity<String> {

    void setFailedEvents(List<EventKey> events);

    List<EventKey> getFailedEvents();

}
