package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;

import java.time.ZonedDateTime;

/**
 * Created by smalik3 on 3/29/16
 */
public interface EventLookup extends Entity<EventLookupDO.EventLookupKey> {

    ZonedDateTime getBucketId();

    void setBucketId(ZonedDateTime bucketId);

    int getShard();

    void setShard(int shard);

}
