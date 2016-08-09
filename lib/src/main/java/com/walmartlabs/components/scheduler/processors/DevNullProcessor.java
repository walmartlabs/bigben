package com.walmartlabs.components.scheduler.processors;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventResponseMixin;
import org.apache.log4j.Logger;

import static com.walmart.marketplace.messages.v1_bigben.EventResponse.Status.PROCESSED;

/**
 * Created by smalik3 on 6/21/16
 */
public class DevNullProcessor implements EventProcessor<Event> {

    private static final Logger L = Logger.getLogger(DevNullProcessor.class);

    private static final ListenableFuture<Event> DONE = Futures.immediateFuture(null);

    @Override
    public ListenableFuture<Event> process(Event event) {
        if (event instanceof EventResponseMixin)
            L.warn("routing event response with no tenant to /dev/null: " + ((EventResponseMixin) event).getEventResponse());
        else
            L.warn("routing event with no tenant to /dev/null: " + event);
        event.setError("/dev/null");
        event.setStatus(PROCESSED.name());
        return DONE;
    }
}
