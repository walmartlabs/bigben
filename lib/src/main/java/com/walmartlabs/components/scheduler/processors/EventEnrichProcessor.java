package com.walmartlabs.components.scheduler.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventLookup;
import com.walmartlabs.components.scheduler.entities.EventLookupDO.EventLookupKey;
import org.apache.log4j.Logger;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmartlabs.components.scheduler.entities.Status.PROCESSED;

/**
 * Created by smalik3 on 8/7/16
 */
public class EventEnrichProcessor implements EventProcessor<Event> {

    private static final Logger L = Logger.getLogger(EventEnrichProcessor.class);

    private static final Selector<EventLookupKey, EventLookup> FULL_SELECTOR = fullSelector(new EventLookupKey("", ""));
    private static final ListenableFuture<Event> SUCCESS = immediateFuture(null);

    protected final DataManager<EventLookupKey, EventLookup> lookupDataManager;
    protected final ProcessorRegistry processorRegistry;

    public EventEnrichProcessor(ProcessorRegistry processorRegistry, DataManager<EventLookupKey, EventLookup> lookupDataManager) {
        this.processorRegistry = processorRegistry;
        this.lookupDataManager = lookupDataManager;
    }

    @Override
    public ListenableFuture<Event> process(Event event) {
        return transformAsync(lookupDataManager.getAsync(new EventLookupKey(event.getXrefId(), event.getTenant()), FULL_SELECTOR), el -> {
            if (el == null) {
                L.debug("no look up found for event, ignoring this event" + event.id());
                event.setStatus(PROCESSED.name());
                return SUCCESS;
            } else {
                event.setPayload(el.getPayload());
                return processorRegistry.process(event);
            }
        });
    }
}
