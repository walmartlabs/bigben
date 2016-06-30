package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;

import java.util.Date;

/**
 * Created by smalik3 on 3/18/16
 */
public interface Bucket extends Entity<Long> {

    enum BucketStatus {PROCESSED, ERROR, UN_PROCESSED, PROCESSING, TIMED_OUT}

    String getStatus();

    void setStatus(String status);

    long getCount();

    void setCount(long count);

    void setProcessedAt(Date date);

    Date getProcessedAt();
}
