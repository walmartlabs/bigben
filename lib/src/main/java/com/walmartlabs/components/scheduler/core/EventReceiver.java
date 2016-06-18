package com.walmartlabs.components.scheduler.core;

import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.model.Bucket;
import com.walmartlabs.components.scheduler.model.BucketDO;
import com.walmartlabs.components.scheduler.model.Event;
import com.walmartlabs.components.scheduler.model.EventLookup;
import com.walmartlabs.components.scheduler.model.EventLookupDO.EventLookupKey;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.AbstractDAO.implClass;
import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static java.lang.String.format;

/**
 * Created by smalik3 on 3/23/16
 */
public class EventReceiver {

    public EventReceiver() {
        System.out.println("here");
    }

    private static final Logger L = Logger.getLogger(EventReceiver.class);

    /*@Autowired
    private Ignite ignite;*/

    @Autowired
    private DataManager<EventKey, Event> dataManager;

    @Autowired
    private DataManager<EventLookupKey, EventLookup> lookupDataManager;

    public void addEvent(Event entity) {
        /*try {
            entity.id().setBucketId(bucketize(entity.id().getEventTime()));
            L.debug(format("%s, add-event: bucket-table: insert, %s", entity.id(), entity));
            final IgniteCache<Long, Bucket> cache = ignite.cache(BUCKET_CACHE);
            final long bucketOffset = toOffset(bucketize(entity.id().getEventTime()));
            L.debug(format("%s, event-time: %d -> bucket-offset: %d", entity.id(), entity.id().getEventTime(), bucketOffset));
            L.debug(format("%s, adjusting counts", entity.id()));
            final Long shardIndex = cache.invoke(bucketOffset, new CountIncrementer(), entity, bucketOffset);
            final EventKey eventKey = EventKey.of(toAbsolute(bucketOffset), shardIndex.intValue(), entity.id().getEventTime(), entity.id().getEventId());
            L.debug(format("%s, add-event: event-table: insert", eventKey));
            final Event e = DataManager.entity(Event.class, eventKey);
            e.setStatus(UN_PROCESSED.name());
            dataManager.insert(e);
            L.debug(format("%s, add-event: event-table: insert: successful", eventKey));
            L.debug(format("%s, add-event: event-lookup-table: insert", eventKey));
            final EventLookup lookupEntity = DataManager.entity(EventLookup.class, EventLookupKey.of(eventKey.getEventTime(), eventKey.getEventId()));
            lookupEntity.setOffset(eventKey.getBucketId());
            lookupEntity.setShard(eventKey.getShard());
            lookupDataManager.insert(lookupEntity);
            L.debug(format("%s, add-event: event-lookup-table: insert: successful", eventKey));
        } catch (Exception e1) {
            e1.printStackTrace();
        }*/
    }

    public static class CountIncrementer implements EntryProcessor<Long, Bucket, Long> {
        @Override
        public Long process(MutableEntry<Long, Bucket> entry, Object... arguments) throws EntryProcessorException {
            final Event entity = (Event) arguments[0];
            final long bucketOffset = (long) arguments[1];
            final Bucket e = entry.getValue() == null ? new BucketDO() : entry.getValue();
            L.debug(format("%s, bucket-offset: %d, old-count: %d, new-count: %d ", entity.id(), bucketOffset, e.getCount(), e.getCount() + 1));
            e.setCount(e.getCount() + 1);
            entry.setValue(e);
            L.debug(format("%s, add-event: bucket-table: update: successful", entity.id()));
            final int shardSize = PROPS.getInteger("event.shard.size", 1000);
            return e.getCount() / shardSize;
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
