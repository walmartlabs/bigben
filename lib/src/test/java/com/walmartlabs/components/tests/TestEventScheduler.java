package com.walmartlabs.components.tests;

import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.core.EventProcessor;
import com.walmartlabs.components.scheduler.core.EventReceiver;
import com.walmartlabs.components.scheduler.model.Bucket;
import com.walmartlabs.components.scheduler.model.Event;
import com.walmartlabs.components.scheduler.model.EventDO;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;
import com.walmartlabs.components.scheduler.services.EventService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.System.currentTimeMillis;
import static java.time.LocalDateTime.now;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;

/**
 * Created by smalik3 on 3/9/16
 */
@ContextConfiguration(locations = "/test-scheduler.xml")
public class TestEventScheduler extends AbstractTestNGSpringContextTests implements EventProcessor<Event> {

    static {
        System.setProperty("dm.entity.packages.scan", "com.walmartlabs.components.scheduler.model");
        System.setProperty("com.walmart.platform.config.runOnEnv", "dev");
        System.setProperty("ips.list", "file://ips.json");
    }

    @Autowired
    private DataManager<EventKey, Event> dataManager;

    @Autowired
    private DataManager<Long, Bucket> bucketDM;

    @Autowired
    private EventReceiver eventReceiver;

    @Autowired
    private EventService eventService;

    @Override
    public ListenableFuture<Event> process(Event event) {
        System.out.println("processing event: " + event);
        events.remove(event.id().toString());
        if (events.size() == 0) {
            System.out.println("test done");
        }
        return immediateFuture(event);
    }

    private static final Map<String, Boolean> events = new ConcurrentHashMap<>();

    @Test(enabled = false)
    public void testEventScheduler2() throws Exception {
        eventService.generateEvents("2016-06-21T18:30Z", "2016-06-21T18:40Z", 10000, "0");
        Thread.sleep(1000000);
    }

    @Test(enabled = false)
    public void testEventScheduler() throws InterruptedException {
        //final ListeningExecutorService executorService = listeningDecorator(newFixedThreadPool(getRuntime().availableProcessors()));
        final Random random = new Random();
        final ZonedDateTime now = ZonedDateTime.now(UTC);
        for (int i = 0; i < 1000; i++) {
            final EventDO entity = new EventDO();
            final ZonedDateTime nextMinute = now.plusMinutes(2).withSecond(0).withNano(0);
            entity.setEventKey(EventKey.of(0, 0, nextMinute.plusSeconds(random.nextInt(60)).toInstant().toEpochMilli(), "EId#" + i));
            System.out.println("event: " + entity.id());
            events.put(entity.id().toString(), true);
            eventReceiver.addEvent(null);
        }
        System.out.println("all events added");
        Thread.sleep(1000000);
    }

    @Test(enabled = false)
    public void testEventScheduler1() throws InterruptedException {
        if (System.getProperty("run.it") != null) {
            System.out.println("loading data");
            final int count = Integer.getInteger("count", 10);
            final long now = now().withMinute(0).withSecond(0).withNano(0).atZone(systemDefault()).toInstant().toEpochMilli();
            //final ExecutorService executorService = new ThreadPoolExecutor(100, 100, 60, MINUTES, new LinkedBlockingQueue<>());
            final AtomicInteger i = new AtomicInteger();
            final Random random = new Random();
            final List<Callable<Object>> tasks = nCopies(count, 0L).stream().map($ -> (Callable<Object>) () -> {
                final EventDO entity = new EventDO();
                entity.setEventKey(EventKey.of(0, 0, now + random.nextInt(3600) * 1000, "EId#" + i.get() + random.nextInt(1_000_000)));
                eventReceiver.addEvent(null);
                return null;
            }).collect(toList());
            //executorService.invokeAll(tasks);
            try {
                tasks.get(0).call();
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("done: " + (currentTimeMillis() - now) + "ms");
        } else System.out.println("not loading data");
        Thread.sleep(1000000);
    }
}
