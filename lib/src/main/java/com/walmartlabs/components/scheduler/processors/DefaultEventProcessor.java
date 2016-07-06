package com.walmartlabs.components.scheduler.processors;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmartlabs.components.scheduler.entities.Event;
import org.apache.log4j.Logger;

/**
 * Created by smalik3 on 6/21/16
 */
public class DefaultEventProcessor implements EventProcessor<Event> {

    private static final Logger L = Logger.getLogger(DefaultEventProcessor.class);

    private static final ListenableFuture<Event> DONE = Futures.immediateFuture(null);

    @Override
    public ListenableFuture<Event> process(Event event) {
        L.debug("Done: " + event.id());
        return DONE;
    }
}
