package com.walmartlabs.components.scheduler.core.hz;

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
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.model.Bucket;
import com.walmartlabs.components.scheduler.model.BucketDO;
import com.walmartlabs.components.scheduler.model.Event;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;
import com.walmartlabs.components.scheduler.model.EventLookup;
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
import static com.walmart.gmp.ingestion.platform.framework.data.core.AbstractDAO.implClass;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static com.walmartlabs.components.scheduler.core.hz.ObjectFactory.OBJECT_ID.EVENT_RECEIVER_ADD_EVENT;
import static com.walmartlabs.components.scheduler.core.hz.ObjectFactory.SCHEDULER_FACTORY_ID;
import static com.walmartlabs.components.scheduler.core.hz.ScheduleScanner.BUCKET_CACHE;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.UN_PROCESSED;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.bucketize;
import static java.lang.String.format;

/**
 * Created by smalik3 on 3/23/16
 */
public class HzEventReceiver {

    private static final Logger L = Logger.getLogger(HzEventReceiver.class);

    @Autowired
    private DataManager<EventKey, Event> dataManager;

    @Autowired
    private Hz hz;

    @Autowired
    private DataManager<EventLookupKey, EventLookup> lookupDataManager;

    static final CountIncrementer CACHED_PROCESSOR = new CountIncrementer();

    public ListenableFuture<EventKey> addEvent(Event entity) {
        final long bucketId = bucketize(entity.id().getEventTime());
        entity.id().setBucketId(bucketId);
        L.debug(format("%s, event-time: %d -> bucket-id: %d", entity.id(), entity.id().getEventTime(), bucketId));
        L.debug(format("%s, add-event: bucket-table: insert, %s", entity.id(), entity));
        final IMap<Long, Bucket> cache = hz.hz().getMap(BUCKET_CACHE);
        return transformAsync(adapt(cache.submitToKey(bucketId, CACHED_PROCESSOR)), (AsyncFunction<Long, EventKey>) count -> {
            final int shardIndex = (int) (count / PROPS.getInteger("event.shard.size", 1000));
            final EventKey eventKey = EventKey.of(bucketId, shardIndex, entity.id().getEventTime(), entity.id().getEventId());
            L.debug(format("%s, add-event: event-table: insert", eventKey));
            final Event e = entity(Event.class, eventKey);
            e.setStatus(UN_PROCESSED.name());
            L.debug(format("%s, add-event: event-lookup-table: insert", eventKey));
            final EventLookup lookupEntity = entity(EventLookup.class, EventLookupKey.of(eventKey.getEventTime(), eventKey.getEventId()));
            lookupEntity.setOffset(eventKey.getBucketId());
            lookupEntity.setShard(eventKey.getShard());
            return transform(allAsList(dataManager.insertAsync(e), lookupDataManager.insertAsync(lookupEntity)), (Function<List<Entity<?>>, EventKey>) $ -> {
                L.debug(format("%s, add-event: successful", eventKey));
                return eventKey;
            });
        });
    }

    private static class CountIncrementer extends AbstractIDSGMPEntryProcessor<Long, Bucket> {
        @Override
        public Long process(Entry<Long, Bucket> entry) throws EntryProcessorException {
            final Bucket b = entry.getValue() == null ? new BucketDO() : entry.getValue();
            b.setCount(b.getCount() + 1);
            entry.setValue(b);
            L.debug(format("bucket-id: %d, old-count: %d, new-count: %d ", entry.getKey(), b.getCount(), b.getCount() + 1));
            return b.getCount();
        }

        @Override
        public int getFactoryId() {
            return SCHEDULER_FACTORY_ID;
        }

        @Override
        public int getId() {
            return EVENT_RECEIVER_ADD_EVENT.ordinal();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    public void removeEvent(Event entity) {
        @SuppressWarnings("unchecked")
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
        L.debug(format("%s, delete-event: successful", entity.id()));
    }
}
