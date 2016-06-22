package com.walmartlabs.components.scheduler.services;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.walmartlabs.components.scheduler.core.EventReceiver;
import com.walmartlabs.components.scheduler.model.EventDO;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.nCopies;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * Created by smalik3 on 3/21/16
 */
@Path("/events")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class EventService {

    @Autowired
    private EventReceiver eventReceiver;

    @GET
    @Path("/stats")
    public Map<Long, Integer> getStats() {
        return ImmutableMap.of(1L, 0);
    }

    @POST
    @Path("/add")
    public void addEvent(EventKey eventKey) {
        final EventDO entity = new EventDO();
        entity.setEventKey(eventKey);
        eventReceiver.addEvent(entity);
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
        transform(successfulAsList(nCopies(numEvents, 0).stream().map($ -> {
            final EventDO entity = new EventDO();
            final ZonedDateTime t = t1.plus(random.nextLong(delta), MILLIS);
            entity.setEventKey(EventKey.of(0, 0, t.toInstant().toEpochMilli(), "EId#" + randomUUID().toString()));
            entity.setTenant(tenant);
            return eventReceiver.addEvent(entity);
        }).collect(toList())), (Function<List<EventKey>, Object>) l -> {
            l.forEach(e -> {
                if (!map.containsKey(e.getBucketId()))
                    map.put(e.getBucketId(), 0);
                map.put(e.getBucketId(), map.get(e.getBucketId()) + 1);
            });
            return null;
        }).get(30, MINUTES);
        return map;
    }

    public void removeEvent(long time, String id) {

    }

    public static void main(String[] args) {
        System.out.println(ZonedDateTime.now(ZoneOffset.UTC));
    }
}
