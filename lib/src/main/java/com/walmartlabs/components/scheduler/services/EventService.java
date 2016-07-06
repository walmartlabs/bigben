package com.walmartlabs.components.scheduler.services;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.hazelcast.core.Member;
import com.walmart.gmp.ingestion.platform.framework.core.Hz;
import com.walmart.gmp.ingestion.platform.framework.core.Props;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventDO.EventKey;
import com.walmartlabs.components.scheduler.entities.EventLookup;
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
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getStackTraceString;
import static com.walmartlabs.components.scheduler.core.ScheduleScanner.EVENT_SCHEDULER;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.bucketize;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.utc;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
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
    @Path("/stats")
    public Map<String, String> getStats() {
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
    public EventResponse addEvent(EventRequest eventRequest) throws Exception {
        return eventReceiver.addEvent(eventRequest).get(Props.PROPS.getInteger("event.service.add.max.wait.time", 60), SECONDS);
    }

    @POST
    @Path("/generate")
    public Map<ZonedDateTime, Integer> generateEvents(BulkEventGeneration bEG) throws Exception {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final ZonedDateTime t1 = ZonedDateTime.parse(bEG.getStartTime());
        final ZonedDateTime t2 = t1.plusMinutes(bEG.getPeriod());
        L.info(String.format("creating %d events between %s and %s for tenant: %s", bEG.getNumEvents(), t1, t2, bEG.getTenantId()));
        long delta = MILLIS.between(t1, t2);
        final Map<ZonedDateTime, Integer> map = new HashMap<>();
        final Integer scanInterval = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
        Futures.transform(successfulAsList(nCopies(bEG.getNumEvents(), 0).stream().map($ -> {
            final EventRequest eventRequest = new EventRequest();
            final ZonedDateTime t = t1.plus(random.nextLong(delta), MILLIS);
            eventRequest.setTenant(bEG.getTenantId());
            eventRequest.setUtc(t.toInstant().toEpochMilli());
            return eventReceiver.addEvent(eventRequest);
        }).collect(toList())), (Function<List<EventResponse>, Object>) l -> {
            l.forEach(e -> {
                final ZonedDateTime bucketId = utc(bucketize(e.getUtc(), scanInterval));
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
    public Map<String, Object> registerProcessor(ProcessorConfig config) {
        try {
            final ProcessorConfig previous = processorRegistry.register(config);
            return ImmutableMap.of("status", "SUCCESS", "previous", previous, "current", config);
        } catch (Exception e) {
            L.error("error in registering processor: " + config, e);
            return ImmutableMap.of("status", "FAIL", "error", getRootCause(e).getStackTrace());
        }
    }

    /*@Autowired
    private DataManager<EventKey, Event> dm;

    @Autowired
    private DataManager<String, EventLookup> dm;

    @POST
    @Path("/dryrun")
    public EventResponse dryrun(@QueryParam("id") String id, @QueryParam("xrefId") String xrefId) {
        if (id != null && id.trim().length() > 0) {

        }
    }*/

    @POST
    @Path("/_receive_")
    public Map<String, String> receive(EventResponse eventResponse) {
        L.debug("received event response: " + eventResponse);
        return ImmutableMap.of("status", "received");
    }

    public void removeEvent(long time, String id) {

    }
}
