package com.walmartlabs.components.scheduler.model;

import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmartlabs.components.scheduler.model.EventLookupDO.EventLookupKey;

/**
 * Created by smalik3 on 3/29/16
 */
public interface EventLookupEntity extends Entity<EventLookupKey> {

    long getOffset();

    void setOffset(long offset);

    int getShard();

    void setShard(int shard);

}
