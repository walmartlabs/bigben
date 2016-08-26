package com.walmartlabs.components.scheduler.services;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.Member;
import com.walmart.gmp.ingestion.platform.framework.core.Hz;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.jobs.SerializableCallable;
import com.walmart.gmp.ingestion.platform.framework.shutdown.SupportsShutdown;
import com.walmart.marketplace.messages.v1_bigben.EventRequest;
import com.walmart.marketplace.messages.v1_bigben.EventResponse;
import com.walmart.marketplace.messages.v1_bigben.EventResponse.Status;
import com.walmart.platform.kernel.exception.error.Error;
import com.walmartlabs.components.core.services.Service;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventDO.EventKey;
import com.walmartlabs.components.scheduler.entities.EventLookup;
import com.walmartlabs.components.scheduler.entities.EventLookupDO.EventLookupKey;
import com.walmartlabs.components.scheduler.input.EventReceiver;
import com.walmartlabs.components.scheduler.processors.ProcessorConfig;
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import com.walmartlabs.components.scheduler.tasks.ShutdownTask;
import com.walmartlabs.components.scheduler.tasks.StatusTask;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.ListenableFutureAdapter.adapt;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.core.SpringContext.spring;
import static com.walmart.marketplace.messages.v1_bigben.EventRequest.Mode.ADD;
import static com.walmart.marketplace.messages.v1_bigben.EventResponse.Status.REJECTED;
import static com.walmart.platform.kernel.exception.error.ErrorCategory.APPLICATION;
import static com.walmart.platform.kernel.exception.error.ErrorSeverity.ERROR;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getStackTraceString;
import static com.walmartlabs.components.scheduler.core.ScheduleScanner.EVENT_SCHEDULER;
import static com.walmartlabs.components.scheduler.input.EventReceiver.toEvent;
import static com.walmartlabs.components.scheduler.utils.EventUtils.fromRequest;
import static com.walmartlabs.components.scheduler.utils.EventUtils.toResponse;
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
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
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
public class EventService implements SupportsShutdown {

    private static final Logger L = Logger.getLogger(EventService.class);

    @Autowired
    private EventReceiver eventReceiver;

    @Autowired
    private Hz hz;

    @Autowired
    private Service service;

    @Autowired
    private ProcessorRegistry processorRegistry;

    @GET
    @Path("/ping")
    public Map<String, String> ping() {
        return ImmutableMap.of("status", "OK");
    }

    @GET
    @Path("/cluster")
    public Map<String, String> cluster() {
        ensureNotShutdown();
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
    @Path("/submit")
    public Response process(List<EventRequest> eventRequests) throws Exception {
        ensureNotShutdown();
        try {
            final List<EventResponse> eventResponses = successfulAsList(eventRequests.stream().map(
                    e -> e.getMode() == ADD ? eventReceiver.addEvent(e) : eventReceiver.removeEvent(e.getId(), e.getTenant())
            ).collect(toList())).get(PROPS.getInteger("event.service.response.max.wait.time", 60), SECONDS);
            long errors = eventResponses.stream().filter(er -> REJECTED.equals(er.getStatus())).count();
            if (errors == eventResponses.size()) return status(BAD_REQUEST).entity(eventResponses).build();
            else if (errors > 0) return status(PARTIAL_CONTENT).entity(eventResponses).build();
            else return ok().entity(eventResponses).build();
        } catch (Exception e) {
            final EventResponse eventResponse = new EventResponse();
            final Throwable rootCause = getRootCause(e);
            eventResponse.setErrors(newArrayList(new Error("500", "submit", getStackTraceString(rootCause), rootCause.getMessage(), ERROR, APPLICATION)));
            return status(INTERNAL_SERVER_ERROR).entity(eventResponse).build();
        }
    }

    @POST
    @Path("/generate")
    public Map<ZonedDateTime, Integer> generateEvents(BulkEventGeneration bEG) throws Exception {
        ensureNotShutdown();
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
            final ListenableFuture<EventResponse> f = eventReceiver.addEvent(eventRequest);
            return transformAsync(f, e -> {
                if (!Status.ACCEPTED.equals(e.getStatus())) {
                    processorRegistry.process(toEvent(e));
                }
                return f;
            });
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
    public Map<String, String> shutdownAPI() {
        ensureNotShutdown();
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

    @POST
    @Path("/register")
    public Response registerProcessor(ProcessorConfig config) {
        ensureNotShutdown();
        try {
            successfulAsList(transformValues(
                    hz.hz().getExecutorService(EVENT_SCHEDULER).submitToAllMembers(
                            (SerializableCallable<ProcessorConfig>) () -> spring().getBean(ProcessorRegistry.class).register(config)),
                    new Function<Future<ProcessorConfig>, ListenableFuture<ProcessorConfig>>() {
                        @Override
                        public ListenableFuture<ProcessorConfig> apply(Future<ProcessorConfig> f) {
                            return adapt(f);
                        }
                    }).values()).get(PROPS.getInteger("processor.register.timeout", 60), SECONDS);
            return ok(ImmutableMap.of("status", "SUCCESS")).build();
        } catch (Exception e) {
            L.error("error in registering processor: " + config, e);
            return status(INTERNAL_SERVER_ERROR).entity(ImmutableMap.of("status", "FAIL", "error", getRootCause(e).getStackTrace())).build();
        }
    }

    @GET
    @Path("/processorRegistry")
    public Response registeredTenants() {
        ensureNotShutdown();
        return ok().entity(processorRegistry.registeredConfigs()).build();
    }

    @Autowired
    @Qualifier("bigbenDataManager")
    private DataManager<EventKey, Event> eventDM;

    @Autowired
    @Qualifier("bigbenDataManager")
    private DataManager<EventLookupKey, EventLookup> lookUpDM;

    @GET
    @Path("/find")
    public Response find(@QueryParam("id") String id, @QueryParam("tenant") String tenant) {
        ensureNotShutdown();
        final EventRequest eventRequest = new EventRequest();
        eventRequest.setId(id);
        eventRequest.setTenant(tenant);
        return find(eventRequest, false);
    }

    @POST
    @Path("/dryrun")
    public Response dryrun(@QueryParam("id") String id, @QueryParam("tenant") String tenant) {
        ensureNotShutdown();
        final EventRequest eventRequest = new EventRequest();
        eventRequest.setId(id);
        eventRequest.setTenant(tenant);
        return find(eventRequest, true);
    }

    private Response find(EventRequest eventRequest, boolean fire) {
        ensureNotShutdown();
        final EventResponse eventResponse = fromRequest(eventRequest);
        if (eventRequest != null && eventRequest.getId() != null && eventRequest.getId().trim().length() > 0) {
            final EventLookupKey eventLookupKey = new EventLookupKey(eventRequest.getId(), eventRequest.getTenant());
            final EventLookup eventLookup = lookUpDM.get(eventLookupKey);
            if (eventLookup == null) {
                return status(NOT_FOUND).entity(eventResponse).build();
            } else {
                final Event event = eventDM.get(EventKey.of(eventLookup.getBucketId(), eventLookup.getShard(), eventLookup.getEventTime(), eventLookup.getEventId()));
                if (event == null) {
                    eventResponse.setEventId(eventLookup.getEventId());
                    eventResponse.setEventTime(eventLookup.getEventTime().toString());
                    return status(NOT_FOUND).entity(eventResponse).build();
                } else {
                    event.setPayload(eventLookup.getPayload());
                    if (fire) {
                        processorRegistry.process(event);
                    }
                    return status(OK).entity(toResponse(event)).build();
                }
            }
        } else {
            eventResponse.setErrors(newArrayList(new Error("400.BIGBEN", "id", "id null", "id null")));
            return status(BAD_REQUEST).entity(eventResponse).build();
        }
    }

    @POST
    @Path("/_receive_")
    @Produces(TEXT_PLAIN)
    public String receive(EventResponse eventResponse, @Context HttpHeaders httpHeaders) {
        ensureNotShutdown();
        L.debug("received event response: " + eventResponse);
        System.out.println(httpHeaders);
        return "done";
    }

    @Override
    public String name() {
        return "EventService";
    }

    @Override
    public int priority() {
        return 0;
    }

    private volatile boolean isShutdown = false;

    @Override
    public ListenableFuture<?> shutdown() {
        isShutdown = true;
        return immediateFuture(null);
    }

    private void ensureNotShutdown() {
        if (isShutdown)
            throw new WebApplicationException(status(400).entity("system has been shutdown").build());
    }
}
