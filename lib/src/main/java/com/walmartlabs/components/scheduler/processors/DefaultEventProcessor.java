package com.walmartlabs.components.scheduler.processors;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmartlabs.components.scheduler.core.EventProcessor;
import com.walmartlabs.components.scheduler.model.Event;

/**
 * Created by smalik3 on 6/21/16
 */
public class DefaultEventProcessor implements EventProcessor<Event> {

    private static final ListenableFuture<Event> DONE = Futures.immediateFuture(null);

    @Override
    public ListenableFuture<Event> process(Event event) {
        System.out.println("Done: " + event.id());
        return DONE;
    }
}
