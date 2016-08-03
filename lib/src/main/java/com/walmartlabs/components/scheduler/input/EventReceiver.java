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
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import info.archinnov.achilles.persistence.AsyncManager;
import info.archinnov.achilles.type.Empty;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.InitializingBean;
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
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.raw;
import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.selector;
import static com.walmart.platform.kernel.exception.error.ErrorCategory.APPLICATION;
import static com.walmart.platform.kernel.exception.error.ErrorSeverity.ERROR;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.*;
import static com.walmartlabs.components.scheduler.core.ScheduleScanner.BUCKET_CACHE;
import static com.walmartlabs.components.scheduler.entities.EventResponse.fromRequest;
import static com.walmartlabs.components.scheduler.entities.ObjectFactory.OBJECT_ID.EVENT_RECEIVER_ADD_EVENT;
import static com.walmartlabs.components.scheduler.entities.ObjectFactory.SCHEDULER_FACTORY_ID;
import static com.walmartlabs.components.scheduler.entities.Status.*;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.*;
import static java.lang.String.format;
import static java.time.ZonedDateTime.parse;
import static java.util.UUID.randomUUID;

/**
 * Created by smalik3 on 3/23/16
 */
public class EventReceiver implements InitializingBean {

    private static final Logger L = Logger.getLogger(EventReceiver.class);

    @Autowired
    private DataManager<EventKey, Event> dataManager;

    @Autowired
    private Hz hz;

    @Autowired
    private ProcessorRegistry processorRegistry;

    private int scanInterval;

    @Override
    public void afterPropertiesSet() throws Exception {
        scanInterval = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
    }

    @Autowired
    private DataManager<EventLookupKey, EventLookup> lookupDataManager;

    public static final CountIncrementer CACHED_PROCESSOR = new CountIncrementer();
    private static final Selector<EventLookupKey, EventLookup> LOOKUP_SELECTOR = selector(EventLookup.class, EventLookup::getBucketId);

    public ListenableFuture<EventResponse> addEvent(EventRequest eventRequest) {
        final ListenableFuture<EventResponse> failed = validate(eventRequest);
        if (failed != null) return failed;

        final long eventTimeMillis = parse(eventRequest.getEventTime()).toInstant().toEpochMilli();
        final ZonedDateTime bucketId = utc(bucketize(eventTimeMillis, scanInterval));

        final EventLookupKey eventLookupKey = new EventLookupKey(eventRequest.getId(), eventRequest.getTenant());
        return catching(transformAsync(lookupDataManager.getAsync(eventLookupKey, LOOKUP_SELECTOR), el -> {
            if (el != null) {
                if (el.getEventTime().toInstant().toEpochMilli() == eventTimeMillis) {
                    final EventKey eventKey = EventKey.of(el.getBucketId(), el.getShard(), el.getEventTime(), el.getEventId());
                    L.debug(format("%s, event update received, no change in event time", eventKey));
                    final Event e = entity(Event.class, eventKey);
                    e.setTenant(el.id().getTenant());
                    e.setPayload(eventRequest.getPayload());
                    e.setStatus(UN_PROCESSED.name());
                    return transform(dataManager.saveAsync(e), new Function<Event, EventResponse>() {
                        @Override
                        public EventResponse apply(Event $) {
                            L.debug(format("%s, event updated successfully", eventKey));
                            final EventResponse eR = fromRequest(eventRequest);
                            eR.setEventId(eventKey.getEventId());
                            eR.setStatus(UPDATED.name());
                            return eR;
                        }
                    });
                } else {
                    final EventKey eventKey = EventKey.of(el.getBucketId(), el.getShard(), parse(eventRequest.getEventTime()), el.getEventId());
                    L.debug(format("%s, event update received, event time changed, deleting and re-inserting", eventKey));
                    return transformAsync(removeEvent(eventRequest.getId(), eventRequest.getTenant()), er -> {
                        if (er.getErrors() != null && !er.getErrors().isEmpty())
                            L.warn("error in deleting event: " + er);
                        return transform(addEvent(eventRequest), new Function<EventResponse, EventResponse>() {
                            @Override
                            public EventResponse apply(EventResponse er) {
                                if (er.getErrors() != null && !er.getErrors().isEmpty())
                                    L.warn("error in re-inserting event: " + er);
                                else {
                                    L.debug("event updated successfully, " + er);
                                    er.setStatus(UPDATED.name());
                                }
                                return er;
                            }
                        });
                    });
                }
            } else {
                final EventKey eventKey = EventKey.of(bucketId, 0, utc(eventTimeMillis), UUIDUtil.toString(randomUUID()));
                L.debug(format("%s, event-time: %s -> bucket-id: %s", eventKey, eventKey.getEventTime(), bucketId));
                L.debug(format("%s, add-event: bucket-table: insert, %s", eventKey, eventRequest));
                final IMap<ZonedDateTime, Bucket> cache = hz.hz().getMap(BUCKET_CACHE);
                return transformAsync(adapt(cache.submitToKey(bucketId, CACHED_PROCESSOR)), (AsyncFunction<Long, EventResponse>) count -> {
                    eventKey.setShard((int) ((count - 1) / PROPS.getInteger("event.shard.size", 1000)));
                    L.debug(format("%s, add-event: event-table: insert", eventKey));
                    final Event e = entity(Event.class, eventKey);
                    e.setStatus(UN_PROCESSED.name());
                    e.setTenant(eventRequest.getTenant());
                    e.setXrefId(eventRequest.getId());
                    e.setPayload(eventRequest.getPayload());
                    L.debug(format("%s, add-event: event-lookup-table: insert", eventKey));
                    final EventLookup lookupEntity = entity(EventLookup.class, new EventLookupKey(eventRequest.getId() == null ? eventKey.getEventId() : eventRequest.getId(), eventRequest.getTenant()));
                    lookupEntity.setBucketId(eventKey.getBucketId());
                    lookupEntity.setShard(eventKey.getShard());
                    lookupEntity.setEventTime(eventKey.getEventTime());
                    lookupEntity.setEventId(eventKey.getEventId());
                    return transform(allAsList(dataManager.insertAsync(e), lookupDataManager.insertAsync(lookupEntity)), (Function<List<Entity<?>>, EventResponse>) $ -> {
                        L.debug(format("%s, add-event: successful", eventKey));
                        final EventResponse eventResponse = fromRequest(eventRequest);
                        eventResponse.setEventId(eventKey.getEventId());
                        eventResponse.setStatus(ACCEPTED.name());
                        return eventResponse;
                    });
                });
            }
        }), Exception.class, ex -> {
            final List<Error> errors = getErrorAtServer(ex);
            final EventResponse eventResponse = fromRequest(eventRequest);
            eventResponse.setErrors(errors);
            eventResponse.setStatus(ERROR.name());
            return eventResponse;
        });
    }

    @Nullable
    private ListenableFuture<EventResponse> validate(EventRequest eventRequest) {
        if (eventRequest.getTenant() == null) {
            final EventResponse eventResponse = fromRequest(eventRequest);
            eventResponse.setStatus(REJECTED.name());
            eventResponse.setErrors(newArrayList(new Error("400", "tenant", "", "tenant not present")));
            L.error("event rejected, tenant missing, " + eventRequest);
            return immediateFuture(eventResponse);
        }
        if (eventRequest.getEventTime() == null) {
            final EventResponse eventResponse = fromRequest(eventRequest);
            eventResponse.setStatus(REJECTED.name());
            eventResponse.setErrors(newArrayList(new Error("400", "eventTime", "", "event time not present")));
            L.error("event rejected, event time not present, " + eventRequest);
            return immediateFuture(eventResponse);
        }
        if (!processorRegistry.registeredTenants().contains(eventRequest.getTenant())) {
            final EventResponse eventResponse = fromRequest(eventRequest);
            eventResponse.setStatus(REJECTED.name());
            eventResponse.setErrors(newArrayList(new Error("400", "tenant", "", "tenant not registered / unknown tenant")));
            L.error("event rejected, unknown tenant. Did you register one in the processors.json?, " + eventRequest);
            return immediateFuture(eventResponse);
        }
        try {
            parse(eventRequest.getEventTime());
        } catch (Exception e) {
            final EventResponse eventResponse = fromRequest(eventRequest);
            eventResponse.setStatus(REJECTED.name());
            eventResponse.setErrors(newArrayList(new Error("400", "eventTime", "", "event time can not be parsed. Must be in ISO 8601 format.")));
            L.error("event rejected, bad event time format, " + eventRequest);
            return immediateFuture(eventResponse);
        }
        if (parse(eventRequest.getEventTime()).isBefore(nowUTC())) {
            final EventResponse eventResponse = fromRequest(eventRequest);
            eventResponse.setStatus(PROCESSED.name());
            eventResponse.setProcessedAt(nowUTC().toString());
            L.warn("lapsed event received, processing immediately: " + eventRequest);
            return transform(processorRegistry.getOrCreate(eventRequest.getTenant()).process(toEvent(eventResponse)), (Function<Event, EventResponse>) $ -> eventResponse);
        }
        return null;
    }

    private static class CountIncrementer extends AbstractIDSGMPEntryProcessor<ZonedDateTime, Bucket> {
        @Override
        public Long process(Entry<ZonedDateTime, Bucket> entry) throws EntryProcessorException {
            final Bucket b = entry.getValue() == null ? new BucketDO() : entry.getValue();
            b.setCount(b.getCount() + 1);
            b.setUpdatedAt(nowUTC());
            entry.setValue(b);
            L.debug(format("bucket-id: %s, old-count: %d, new-count: %d ", entry.getKey(), b.getCount() - 1, b.getCount()));
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

    private static final Selector<EventLookupKey, EventLookup> FULL_SELECTOR = fullSelector(new EventLookupKey("", ""));

    public ListenableFuture<EventResponse> removeEvent(String id, String tenant) {
        final EventResponse eventResponse = new EventResponse();
        eventResponse.setId(id);
        eventResponse.setTenant(tenant);
        return catching(transformAsync(lookupDataManager.getAsync(new EventLookupKey(id, tenant), FULL_SELECTOR), eventLookup -> {
            if (eventLookup == null) {
                return immediateFuture(eventResponse);
            }
            @SuppressWarnings("unchecked")
            final CqlDAO<EventKey, Event> evtCqlDAO = (CqlDAO<EventKey, Event>) dataManager.getPrimaryDAO(V1).unwrap();
            final AsyncManager am = evtCqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
            final EventKey eventKey = EventKey.of(eventLookup.getBucketId(), eventLookup.getShard(), eventLookup.getEventTime(), eventLookup.getEventId());
            L.debug("removing event: " + eventKey);
            return transformAsync(am.deleteById(implClass(V1, EventKey.class), eventKey), $ -> {
                L.debug("removing event look up: " + eventLookup);
                return transform(am.deleteById(implClass(V1, EventLookup.class), eventLookup.id()), new Function<Empty, EventResponse>() {
                    @Override
                    public EventResponse apply(Empty $) {
                        L.debug("event removed successfully : " + id);
                        eventResponse.setEventId(eventKey.getEventId());
                        eventResponse.setEventTime(eventKey.getEventTime().toString());
                        eventResponse.setStatus(DELETED.name());
                        eventResponse.setTenant(tenant);
                        return eventResponse;
                    }
                });
            });
        }), Exception.class, ex -> {
            final Throwable cause = getRootCause(ex);
            L.error("error in removing the event: " + id, cause);
            eventResponse.setErrors(newArrayList(new Error("500", id, cause.getMessage(), getStackTraceString(cause), ERROR, APPLICATION)));
            return eventResponse;
        });
    }

    private static final EventKey EMPTY_KEY = new EventKey();

    public static Event toEvent(EventResponse e) {
        final Event event = raw(entity(Event.class, EMPTY_KEY));
        event.setTenant(e.getTenant());
        event.setError(null);
        ((EventResponseMixin) event).setEventResponse(e);
        e.setProcessedAt(nowUTC().toString());
        return event;
    }
}
