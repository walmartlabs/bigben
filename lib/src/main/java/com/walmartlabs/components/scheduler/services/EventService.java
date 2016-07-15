package com.walmartlabs.components.scheduler.services;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.Member;
import com.walmart.gmp.ingestion.platform.framework.core.Hz;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.platform.kernel.exception.error.Error;
import com.walmartlabs.components.core.services.Service;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventDO.EventKey;
import com.walmartlabs.components.scheduler.entities.EventLookup;
import com.walmartlabs.components.scheduler.entities.EventLookupDO.EventLookupKey;
import com.walmartlabs.components.scheduler.entities.EventRequest;
import com.walmartlabs.components.scheduler.entities.EventResponse;
import com.walmartlabs.components.scheduler.input.EventReceiver;
import com.walmartlabs.components.scheduler.processors.ProcessorConfig;
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import com.walmartlabs.components.scheduler.tasks.ShutdownTask;
import com.walmartlabs.components.scheduler.tasks.StatusTask;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.platform.kernel.exception.error.ErrorCategory.APPLICATION;
import static com.walmart.platform.kernel.exception.error.ErrorSeverity.ERROR;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getStackTraceString;
import static com.walmartlabs.components.scheduler.core.ScheduleScanner.EVENT_SCHEDULER;
import static com.walmartlabs.components.scheduler.entities.EventResponse.fromRequest;
import static com.walmartlabs.components.scheduler.entities.EventResponse.toResponse;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.bucketize;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.utc;
import static java.time.ZonedDateTime.parse;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.nCopies;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.*;
import static javax.ws.rs.core.Response.ok;
import static javax.ws.rs.core.Response.status;
import static org.apache.commons.lang3.tuple.Pair.of;

/**
 * Created by smalik3 on 3/21/16
 */
@Path("/events")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class EventService {

    private static final Logger L = Logger.getLogger(EventService.class);

    @Autowired
    private EventReceiver eventReceiver;

    @Autowired
    private Hz hz;

    @Autowired
    private Service service;

    @GET
    @Path("/ping")
    public Map<String, String> ping() {
        return ImmutableMap.of("status", "OK");
    }

    @GET
    @Path("/cluster")
    public Map<String, String> cluster() {
        final Map<Member, Future<String>> results = hz.hz().getExecutorService(EVENT_SCHEDULER).submitToAllMembers(new StatusTask(service.name()));
        final Map<String, String> map = new HashMap<>();
        for (Map.Entry<Member, Future<String>> entry : results.entrySet()) {
            try {
                map.put(entry.getKey().getSocketAddress().toString(), entry.getValue().get());
            } catch (Exception e) {
                map.put(entry.getKey().getSocketAddress().toString(), "Error: " + getStackTraceString(getRootCause(e)));
            }
        }
        return map;
    }

    @POST
    @Path("/add")
    public Response addEvent(EventRequest eventRequest) throws Exception {
        try {
            return ok(eventReceiver.addEvent(eventRequest).get(PROPS.getInteger("event.service.response.max.wait.time", 60), SECONDS)).build();
        } catch (Exception e) {
            final EventResponse eventResponse = fromRequest(eventRequest);
            final Throwable rootCause = getRootCause(e);
            eventResponse.setErrors(newArrayList(new Error("500", "add-event", getStackTraceString(rootCause), rootCause.getMessage(), ERROR, APPLICATION)));
            return status(INTERNAL_SERVER_ERROR).entity(eventResponse).build();
        }
    }

    @POST
    @Path("/bulk/add")
    public Response addEvents(List<EventRequest> eventRequests) throws Exception {
        final List<EventResponse> eventResponses = successfulAsList(eventRequests.stream().map(
                e -> eventReceiver.addEvent(e)).collect(toList())).get(PROPS.getInteger("event.service.response.max.wait.time", 60), SECONDS);
        return ok().entity(eventResponses).build();
    }

    @POST
    @Path("/generate")
    public Map<ZonedDateTime, Integer> generateEvents(BulkEventGeneration bEG) throws Exception {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final ZonedDateTime t1 = parse(bEG.getStartTime());
        final ZonedDateTime t2 = t1.plusMinutes(bEG.getPeriod());
        L.info(String.format("creating %d events between %s and %s for tenant: %s", bEG.getNumEvents(), t1, t2, bEG.getTenantId()));
        long delta = MILLIS.between(t1, t2);
        final Map<ZonedDateTime, Integer> map = new HashMap<>();
        final Integer scanInterval = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
        transform(successfulAsList(nCopies(bEG.getNumEvents(), 0).stream().map($ -> {
            final EventRequest eventRequest = new EventRequest();
            final ZonedDateTime t = t1.plus(random.nextLong(delta), MILLIS);
            eventRequest.setTenant(bEG.getTenantId());
            eventRequest.setEventTime(t.toString());
            eventRequest.setId(randomUUID().toString());
            return eventReceiver.addEvent(eventRequest);
        }).collect(toList())), (Function<List<EventResponse>, Object>) l -> {
            l.forEach(e -> {
                final ZonedDateTime bucketId = utc(bucketize(parse(e.getEventTime()).toInstant().toEpochMilli(), scanInterval));
                if (!map.containsKey(bucketId))
                    map.put(bucketId, 0);
                map.put(bucketId, map.get(bucketId) + 1);
            });
            return null;
        }).get(30, MINUTES);
        return map;
    }

    public static final ShutdownTask SHUTDOWN_TASK = new ShutdownTask();

    @POST
    @Path("/shutdown")
    public Map<String, String> shutdown() {
        final Map<Member, Future<Boolean>> results = hz.hz().getExecutorService(EVENT_SCHEDULER).submitToAllMembers(SHUTDOWN_TASK);
        return results.entrySet().stream().map(e -> {
            try {
                return of(e.getKey().getAddress().getInetAddress().toString(), String.valueOf(e.getValue().get()));
            } catch (Exception e1) {
                final Throwable cause = getRootCause(e1);
                return of(e.getKey().getUuid(), cause.getMessage() + ": " + getStackTraceString(cause));
            }
        }).collect(toMap(Pair::getLeft, Pair::getRight));
    }

    @Autowired
    private ProcessorRegistry processorRegistry;

    @POST
    @Path("/processors/register")
    public Response registerProcessor(ProcessorConfig config) {
        try {
            final ProcessorConfig previous = processorRegistry.register(config);
            return ok(ImmutableMap.of("status", "SUCCESS", "previous", previous, "current", config)).build();
        } catch (Exception e) {
            L.error("error in registering processor: " + config, e);
            return status(INTERNAL_SERVER_ERROR).entity(ImmutableMap.of("status", "FAIL", "error", getRootCause(e).getStackTrace())).build();
        }
    }

    @Autowired
    private DataManager<EventKey, Event> eventDM;

    @Autowired
    private DataManager<EventLookupKey, EventLookup> lookUpDM;

    @GET
    @Path("/{id}")
    public Response find(@PathParam("id") String id) {
        return find(id, false);
    }

    @POST
    @Path("/dryrun/{id}")
    public Response dryrun(@PathParam("id") String id) {
        return find(id, true);
    }

    private Response find(String id, boolean fire) {
        final EventResponse eventResponse = new EventResponse();
        if (id != null && id.trim().length() > 0) {
            final EventLookupKey eventLookupKey = new EventLookupKey(id);
            final EventLookup eventLookup = lookUpDM.get(eventLookupKey);
            if (eventLookup == null) {
                eventResponse.setId(id);
                return status(NOT_FOUND).entity(eventResponse).build();
            } else {
                final Event event = eventDM.get(EventKey.of(eventLookup.getBucketId(), eventLookup.getShard(), eventLookup.getEventTime(), eventLookup.getEventId()));
                if (event == null) {
                    eventResponse.setEventId(eventLookup.getEventId());
                    eventResponse.setEventTime(eventLookup.getEventTime().toString());
                    return status(NOT_FOUND).entity(eventResponse).build();
                } else {
                    if (fire)
                        processorRegistry.getOrCreate(event.getTenant()).process(event);
                    return status(OK).entity(toResponse(event)).build();
                }
            }
        } else {
            eventResponse.setErrors(newArrayList(new Error("400.BIGBEN", id, "id null", "id null")));
            return status(BAD_REQUEST).entity(eventResponse).build();
        }
    }

    @POST
    @Path("/_receive_")
    public Map<String, String> receive(EventResponse eventResponse) {
        L.debug("received event response: " + eventResponse);
        return ImmutableMap.of("status", "received");
    }

    @POST
    @Path("/delete/{id}")
    public Response removeEvent(@PathParam("id") String id) {
        try {
            final EventResponse eventResponse = eventReceiver.removeEvent(id).get(PROPS.getInteger("event.service.response.max.wait.time", 60), SECONDS);
            if (eventResponse.getEventId() == null) {
                return status(NOT_FOUND).entity(eventResponse).build();
            } else {
                return ok().entity(eventResponse).build();
            }
        } catch (Exception e) {
            final Throwable rootCause = getRootCause(e);
            final EventResponse eventResponse = new EventResponse();
            eventResponse.setId(id);
            eventResponse.setErrors(newArrayList(new Error("500", id, getStackTraceString(rootCause), rootCause.getMessage(), ERROR, APPLICATION)));
            return status(INTERNAL_SERVER_ERROR).entity(eventResponse).build();
        }
    }

    @POST
    @Path("/bulk/delete")
    public Response removeEvent(List<String> ids) {
        try {
            return status(OK).entity(successfulAsList(ids.stream().map(id -> eventReceiver.removeEvent(id)).collect(toList())).
                    get(PROPS.getInteger("event.service.response.max.wait.time", 60), SECONDS)).build();
        } catch (Exception e) {
            return status(INTERNAL_SERVER_ERROR).entity(ids.stream().map(id -> {
                final EventResponse eventResponse = new EventResponse();
                eventResponse.setId(id);
                final Throwable rootCause = getRootCause(e);
                eventResponse.setErrors(newArrayList(new Error("500", id, getStackTraceString(rootCause), rootCause.getMessage(), ERROR, APPLICATION)));
                return eventResponse;
            }).collect(toList())).build();
        }
    }
}
