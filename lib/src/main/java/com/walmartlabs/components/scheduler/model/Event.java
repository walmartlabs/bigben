package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;

/**
 * Created by smalik3 on 3/16/16
 */
public interface Event extends Entity<EventKey> {

    String getStatus();

    void setStatus(String status);

    void setError(String error);

    String getError();
}
