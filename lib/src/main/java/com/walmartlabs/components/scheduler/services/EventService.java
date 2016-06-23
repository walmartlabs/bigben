package com.walmartlabs.components.scheduler.services;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.hazelcast.core.Member;
import com.walmart.gmp.ingestion.platform.framework.core.Hz;
import com.walmart.gmp.ingestion.platform.framework.core.Props;
import com.walmartlabs.components.scheduler.core.EventReceiver;
import com.walmartlabs.components.scheduler.core.ScheduleScanner;
import com.walmartlabs.components.scheduler.model.EventRequest;
import com.walmartlabs.components.scheduler.model.EventResponse;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.core.SpringContext.spring;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getStackTraceString;
import static com.walmartlabs.components.scheduler.core.ScheduleScanner.EVENT_SCHEDULER;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.bucketize;
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

    @Autowired
    private EventReceiver eventReceiver;

    @Autowired
    private Hz hz;

    @GET
    @Path("/stats")
    public Map<Long, Integer> getStats() {
        return ImmutableMap.of(1L, 0);
    }

    @POST
    @Path("/add")
    public EventResponse addEvent(EventRequest eventRequest) throws Exception {
        return eventReceiver.addEvent(eventRequest).get(Props.PROPS.getInteger("event.service.add.max.wait.time", 60), SECONDS);
    }

    @POST
    @Path("/generate")
    public Map<Long, Integer> generateEvents(@QueryParam("from") String fromTime, @QueryParam("to") String toTime,
                                             @QueryParam("number") int numEvents, @QueryParam("tenant") String tenant) throws Exception {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final ZonedDateTime t1 = ZonedDateTime.parse(fromTime);
        final ZonedDateTime t2 = ZonedDateTime.parse(toTime);
        long delta = MILLIS.between(t1, t2);
        final Map<Long, Integer> map = new HashMap<>();
        final Integer scanInterval = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
        Futures.transform(successfulAsList(nCopies(numEvents, 0).stream().map($ -> {
            final EventRequest eventRequest = new EventRequest();
            final ZonedDateTime t = t1.plus(random.nextLong(delta), MILLIS);
            eventRequest.setTenant(tenant);
            eventRequest.setUtc(t.toInstant().toEpochMilli());
            return eventReceiver.addEvent(eventRequest);
        }).collect(toList())), (Function<List<EventResponse>, Object>) l -> {
            l.forEach(e -> {
                final long bucketId = bucketize(e.getUtc(), scanInterval);
                if (!map.containsKey(bucketId))
                    map.put(bucketId, 0);
                map.put(bucketId, map.get(bucketId) + 1);
            });
            return null;
        }).get(30, MINUTES);
        return map;
    }

    public Map<String, String> shutdown() {
        final Map<Member, Future<Boolean>> results = hz.hz().getExecutorService(EVENT_SCHEDULER).submitToAllMembers(() -> {
            if (spring() != null) {
                spring().getBean(ScheduleScanner.class).shutdown();
                return true;
            } else {
                return false;
            }
        });
        return results.entrySet().stream().map(e -> {
            try {
                return of(e.getKey().getAddress().getInetAddress().toString(), String.valueOf(e.getValue().get()));
            } catch (Exception e1) {
                final Throwable cause = getRootCause(e1);
                return of(e.getKey().getUuid(), cause.getMessage() + ": " + getStackTraceString(cause));
            }
        }).collect(toMap(Pair::getLeft, Pair::getRight));
    }

    public void removeEvent(long time, String id) {

    }

    public static void main(String[] args) {
        System.out.println(ZonedDateTime.now(ZoneOffset.UTC));
    }
}
