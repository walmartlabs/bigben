package com.walmartlabs.components.scheduler.core;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.walmart.gmp.ingestion.platform.framework.core.AbstractIDSGMPEntryProcessor;
import com.walmart.gmp.ingestion.platform.framework.core.Hz;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmart.platform.kernel.exception.error.Error;
import com.walmart.services.common.util.UUIDUtil;
import com.walmartlabs.components.scheduler.model.*;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;
import com.walmartlabs.components.scheduler.model.EventLookupDO.EventLookupKey;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.cache.processor.EntryProcessorException;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.ListenableFutureAdapter.adapt;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getErrorAtServer;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.UN_PROCESSED;
import static com.walmartlabs.components.scheduler.model.EventResponse.fromRequest;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.bucketize;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;

/**
 * Created by smalik3 on 3/23/16
 */
public class EventReceiver {

    private static final Logger L = Logger.getLogger(EventReceiver.class);

    @Autowired
    private DataManager<EventKey, Event> dataManager;

    @Autowired
    private Hz hz;

    @Autowired
    private DataManager<EventLookupKey, EventLookup> lookupDataManager;

    static final CountIncrementer CACHED_PROCESSOR = new CountIncrementer();

    public ListenableFuture<EventResponse> addEvent(EventRequest entity) {
        final Integer scanInterval = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
        final long bucketId = bucketize(entity.getUtc(), scanInterval);
        final EventKey eventKey = EventKey.of(bucketId, 0, entity.getUtc(), UUIDUtil.toString(randomUUID()));
        L.debug(format("%s, event-time: %d -> bucket-id: %d", eventKey, eventKey.getEventTime(), bucketId));
        L.debug(format("%s, add-event: bucket-table: insert, %s", eventKey, entity));
        final IMap<Long, Bucket> cache = hz.hz().getMap(ScheduleScanner.BUCKET_CACHE);
        return catching(transformAsync(adapt(cache.submitToKey(bucketId, CACHED_PROCESSOR)), (AsyncFunction<Long, EventResponse>) count -> {
            eventKey.setShard((int) (count / PROPS.getInteger("event.shard.size", 1000)));
            L.debug(format("%s, add-event: event-table: insert", eventKey));
            final Event e = entity(Event.class, eventKey);
            e.setStatus(UN_PROCESSED.name());
            e.setTenant(entity.getTenant());
            e.setXrefId(entity.getId());
            e.setPayload(entity.getPayload());
            L.debug(format("%s, add-event: event-lookup-table: insert", eventKey));
            final EventLookup lookupEntity = entity(EventLookup.class, new EventLookupKey(entity.getId() == null ? eventKey.getEventId() : entity.getId(), eventKey.getEventId()));
            lookupEntity.setBucketId(eventKey.getBucketId());
            lookupEntity.setShard(eventKey.getShard());
            return transform(allAsList(dataManager.insertAsync(e), lookupDataManager.insertAsync(lookupEntity)), (Function<List<Entity<?>>, EventResponse>) $ -> {
                L.debug(format("%s, add-event: successful", eventKey));
                return fromRequest(entity);
            });
        }), Exception.class, ex -> {
            final List<Error> errors = getErrorAtServer(ex);
            final EventResponse eventResponse = fromRequest(entity);
            eventResponse.setErrors(errors);
            return eventResponse;
        });
    }

    private static class CountIncrementer extends AbstractIDSGMPEntryProcessor<Long, Bucket> {
        @Override
        public Long process(Entry<Long, Bucket> entry) throws EntryProcessorException {
            final Bucket b = entry.getValue() == null ? new BucketDO() : entry.getValue();
            b.setCount(b.getCount() + 1);
            entry.setValue(b);
            L.debug(format("bucket-id: %d, old-count: %d, new-count: %d ", entry.getKey(), b.getCount() - 1, b.getCount()));
            return b.getCount();
        }

        @Override
        public int getFactoryId() {
            return ObjectFactory.SCHEDULER_FACTORY_ID;
        }

        @Override
        public int getId() {
            return ObjectFactory.OBJECT_ID.EVENT_RECEIVER_ADD_EVENT.ordinal();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    public void removeEvent(Event entity) {
        /*@SuppressWarnings("unchecked")
        final CqlDAO<EventLookupKey, EventLookup> cqlDAO = (CqlDAO<EventLookupKey, EventLookup>) dataManager.getPrimaryDAO(V1).unwrap();
        final EventLookupKey id = EventLookupKey.of(entity.id().getEventTime(), entity.id().getEventId());

        final EventLookup e = lookupDataManager.get(id);
        L.debug(format("%s, delete-event: event-lookup-table", entity.id()));
        cqlDAO.removeById(implClass(V1, EventLookupKey.class), id);

        if (e != null) {
            @SuppressWarnings("unchecked")
            final CqlDAO<EventKey, Event> cqlDAO1 = (CqlDAO<EventKey, Event>) dataManager.getPrimaryDAO(V1).unwrap();
            L.debug(format("%s, delete-event: event-table", entity.id()));
            cqlDAO1.removeById(implClass(V1, EventKey.class), e.id());
        }
        L.debug(format("%s, delete-event: successful", entity.id()));*/
    }
}
