package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;

/**
 * Created by smalik3 on 3/29/16
 */
public interface EventLookup extends Entity<EventLookupDO.EventLookupKey> {

    long getBucketId();

    void setBucketId(long bucketId);

    int getShard();

    void setShard(int shard);

}
