package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;

import java.util.Set;

/**
 * Created by smalik3 on 3/18/16
 */
public interface EventBucketStatusEntity extends Entity<Long> {

    void setError(String s);

    String getError();

    void setJobCount(int jobCount);

    int getJobCount();

    String getFailedShards();

    void setFailedShards(String failedShards);

    void addFailedShards(Set<Integer> failedShards);

    Set<Integer> failedShards();

    enum BucketStatus {PROCESSED, ERROR, UN_PROCESSED}

    String getStatus();

    void setStatus(String status);

    long getCount();

    void setCount(long count);
}
