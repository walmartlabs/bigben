package com.walmartlabs.components.scheduler.input;

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
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmart.platform.kernel.exception.error.Error;
import com.walmart.services.common.util.UUIDUtil;
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.entities.*;
import com.walmartlabs.components.scheduler.entities.EventDO.EventKey;
import com.walmartlabs.components.scheduler.entities.EventLookupDO.EventLookupKey;
import info.archinnov.achilles.persistence.AsyncManager;
import info.archinnov.achilles.type.Empty;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.cache.processor.EntryProcessorException;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.ListenableFutureAdapter.adapt;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.AbstractDAO.implClass;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmart.platform.kernel.exception.error.ErrorCategory.APPLICATION;
import static com.walmart.platform.kernel.exception.error.ErrorSeverity.ERROR;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.*;
import static com.walmartlabs.components.scheduler.core.ScheduleScanner.BUCKET_CACHE;
import static com.walmartlabs.components.scheduler.entities.Bucket.Status.UN_PROCESSED;
import static com.walmartlabs.components.scheduler.entities.EventResponse.fromRequest;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.bucketize;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.utc;
import static java.lang.String.format;
import static java.time.ZonedDateTime.parse;
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

    public static final CountIncrementer CACHED_PROCESSOR = new CountIncrementer();

    public ListenableFuture<EventResponse> addEvent(EventRequest entity) {
        final Integer scanInterval = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
        final long millis = parse(entity.getEventTime()).toInstant().toEpochMilli();
        final ZonedDateTime bucketId = utc(bucketize(millis, scanInterval));
        final EventKey eventKey = EventKey.of(bucketId, 0, utc(millis), UUIDUtil.toString(randomUUID()));
        L.debug(format("%s, event-time: %s -> bucket-id: %s", eventKey, eventKey.getEventTime(), bucketId));
        L.debug(format("%s, add-event: bucket-table: insert, %s", eventKey, entity));
        final IMap<ZonedDateTime, Bucket> cache = hz.hz().getMap(BUCKET_CACHE);
        return catching(transformAsync(adapt(cache.submitToKey(bucketId, CACHED_PROCESSOR)), (AsyncFunction<Long, EventResponse>) count -> {
            eventKey.setShard((int) (count / PROPS.getInteger("event.shard.size", 1000)));
            L.debug(format("%s, add-event: event-table: insert", eventKey));
            final Event e = entity(Event.class, eventKey);
            e.setStatus(UN_PROCESSED.name());
            e.setTenant(entity.getTenant());
            e.setXrefId(entity.getId());
            e.setPayload(entity.getPayload());
            L.debug(format("%s, add-event: event-lookup-table: insert", eventKey));
            final EventLookup lookupEntity = entity(EventLookup.class, new EventLookupKey(entity.getId() == null ? eventKey.getEventId() : entity.getId()));
            lookupEntity.setBucketId(eventKey.getBucketId());
            lookupEntity.setShard(eventKey.getShard());
            lookupEntity.setEventTime(eventKey.getEventTime());
            lookupEntity.setEventId(eventKey.getEventId());
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

    private static class CountIncrementer extends AbstractIDSGMPEntryProcessor<ZonedDateTime, Bucket> {
        @Override
        public Long process(Entry<ZonedDateTime, Bucket> entry) throws EntryProcessorException {
            final Bucket b = entry.getValue() == null ? new BucketDO() : entry.getValue();
            b.setCount(b.getCount() + 1);
            entry.setValue(b);
            L.debug(format("bucket-id: %s, old-count: %d, new-count: %d ", entry.getKey(), b.getCount() - 1, b.getCount()));
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

    private static final Selector<EventLookupKey, EventLookup> FULL_SELECTOR = fullSelector(new EventLookupKey(""));

    public ListenableFuture<EventResponse> removeEvent(String id) {
        final EventResponse eventResponse = new EventResponse();
        eventResponse.setId(id);
        return catching(transformAsync(lookupDataManager.getAsync(new EventLookupKey(id), FULL_SELECTOR), eventLookup -> {
            if (eventLookup == null) {
                return immediateFuture(eventResponse);
            }
            @SuppressWarnings("unchecked")
            final CqlDAO<EventKey, Event> evtCqlDAO = (CqlDAO<EventKey, Event>) dataManager.getPrimaryDAO(V1).unwrap();
            final AsyncManager am = evtCqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
            final EventKey eventKey = EventKey.of(eventLookup.getBucketId(), eventLookup.getShard(), eventLookup.getEventTime(), eventLookup.getEventId());
            L.info("removing event: " + eventKey);
            return transformAsync(am.deleteById(implClass(V1, EventKey.class), eventKey), $ -> {
                L.info("removing event look up: " + eventLookup);
                return transform(am.deleteById(implClass(V1, EventLookup.class), eventLookup.id()), new Function<Empty, EventResponse>() {
                    @Override
                    public EventResponse apply(Empty $) {
                        L.debug("event removed successfully : " + id);
                        eventResponse.setEventId(eventKey.getEventId());
                        eventResponse.setEventTime(eventKey.getEventTime().toString());
                        return eventResponse;
                    }
                });
            });
        }), Exception.class, ex -> {
            final Throwable cause = getRootCause(ex);
            L.error("error in removing the event: " + id, cause);
            eventResponse.setErrors(newArrayList(new Error("BB_001", id, cause.getMessage(), getStackTraceString(cause), ERROR, APPLICATION)));
            return eventResponse;
        });
    }
}
