package com.walmartlabs.components.scheduler.entities;

import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;

import java.time.ZonedDateTime;


/**
 * Created by smalik3 on 3/18/16
 */
public interface Bucket extends Entity<ZonedDateTime> {

    enum Status {PROCESSED, ERROR, UN_PROCESSED, PROCESSING, EMPTY}

    String getStatus();

    void setStatus(String status);

    long getCount();

    void setCount(long count);

    void setProcessedAt(ZonedDateTime date);

    ZonedDateTime getProcessedAt();

    ZonedDateTime getUpdatedAt();

    void setUpdatedAt(ZonedDateTime updatedAt);
}
