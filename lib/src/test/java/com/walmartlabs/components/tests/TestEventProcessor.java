package com.walmartlabs.components.tests;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmartlabs.components.scheduler.core.EventProcessor;
import com.walmartlabs.components.scheduler.model.EventDO;

/**
 * Created by smalik3 on 3/8/16
 */
public class TestEventProcessor implements EventProcessor<EventDO> {
    @Override
    public ListenableFuture<EventDO> process(EventDO s) {
        /*if (s.getPayload().equals("sku_4"))
            throw new IllegalArgumentException("sku_4");
        System.out.println("event processed " + s.getPayload() + " with delay " + s.getDelay(SECONDS));*/
        return Futures.immediateFuture(s);
    }
}
