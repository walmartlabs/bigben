package com.walmartlabs.components.tests;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.MessagePublisher;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.PublisherFactory;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.consumer.KafkaConsumerBean;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.processors.TopicMessageProcessor;
import com.walmart.marketplace.messages.v1_bigben.EventRequest;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventDO;
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import com.walmartlabs.components.scheduler.utils.TimeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.walmart.gmp.ingestion.platform.framework.messaging.kafka.Constants.APP_NAME;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.setProperty;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.now;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by smalik3 on 7/15/16
 */
@ContextConfiguration(locations = {"/test-beans.xml"/*, "/cluster.xml"*/})
public class TestE2E extends AbstractTestNGSpringContextTests {

    static {
        setProperty("com.walmart.platform.config.runOnEnv", "prod");
        setProperty("com.walmart.platform.config.appName", "gmp-solr-consumer");
        setProperty("ccmProps", "bigbenProps");
        setProperty("hz.config", "hz_local");
    }

    @Autowired
    private ProcessorRegistry processorRegistry;

    @Test
    public void testSerialization() throws InterruptedException, ExecutionException, TimeoutException {
        Event sandeep = DataManager.raw(DataManager.entity(Event.class, EventDO.EventKey.of(TimeUtils.nowUTC(), 1, now(), "sandeep")));
        sandeep.setTenant("PROMO/STG");
        sandeep.setPayload("{x:y}");
        processorRegistry.process(sandeep).get(2, HOURS);
    }

    @Test
    public void produce() throws ExecutionException, InterruptedException {
        setProperty(APP_NAME, "gmp-solr-consumer");

        // input:
        final int durationSecs = 5 * 60;
        final int throttling = 1_000_000;
        final int numEvents = 1_000_000;
        final int initialDelay = 4;

        final MessagePublisher<String, EventRequest, Object> publisher = new PublisherFactory("promo_outbound_group", "bigben_promo_kafka", true).create();
        final ZonedDateTime now = now(UTC);
        final ZonedDateTime time = now.plusMinutes(initialDelay).withSecond(0).withNano(0);
        final Random random = new Random();
        final Semaphore throttler = new Semaphore(throttling);
        final ExecutorService executorService = Executors.newFixedThreadPool(getRuntime().availableProcessors());
        ScheduledExecutorService scheduledThreadPoolExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> throttler.release(throttling - throttler.availablePermits()), 0, 1, SECONDS);

        final CountDownLatch l = new CountDownLatch(numEvents);
        final Map<String, Integer> histogram = new HashMap<>();
        final long n = currentTimeMillis();
        for (int i = 0; i < numEvents; i++) {
            throttler.acquire();
            final ZonedDateTime eventTime = time.plusSeconds(random.nextInt(durationSecs));
            final EventRequest eventRequest = new EventRequest();
            eventRequest.setPayload("Payload" + i);
            eventRequest.setTenant("PROMO/KAFKA/STG");
            eventRequest.setEventTime(eventTime.toString());
            eventRequest.setId(randomUUID().toString());
            final ZonedDateTime minutes = eventTime.withSecond(0).withNano(0);
            if (!histogram.containsKey(minutes.toString()))
                histogram.put(minutes.toString(), 0);
            histogram.put(minutes.toString(), histogram.get(minutes.toString()) + 1);
            executorService.submit(() -> {
                final ListenableFuture<Object> f = publisher.publish("", eventRequest);
                addCallback(f, new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object result) {
                        //System.out.println("event published successfully: " + eventRequest.getPayload());
                        l.countDown();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        System.out.println("failed to publish event");
                        t.printStackTrace();
                        l.countDown();
                    }
                });
            });

        }
        l.await();
        System.out.println("done: time to publish " + numEvents + " event = " + (currentTimeMillis() - n) + " ms");
        System.out.println("distribution: ");
        histogram.forEach((k, v) -> System.out.println("bucket: " + k + ", counts: " + v));
    }

    @Test
    public void consume() throws Exception {
        final AtomicInteger count = new AtomicInteger();
        final AtomicReference<Integer> sum = new AtomicReference<>(0);
        ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                System.out.println("message received: " + (count.get() - sum.get()) + ", until now: " + count.get() + ", time: " + new Date());
                sum.set(count.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 10, TimeUnit.SECONDS);
        final TopicMessageProcessor tmp = (s, r) -> {
            //System.out.println(r.value());
            //L.debug("topic: " + s + ", " + r.partition() + ": " + r.offset());
            count.incrementAndGet();
            return immediateFuture(null);
        };
        System.setProperty(APP_NAME, "gmp-solr-consumer");

        final KafkaConsumerBean consumer = new KafkaConsumerBean("bigben_promo_kafka", tmp);
        consumer.afterPropertiesSet();
        new CountDownLatch(1).await();
    }

    /*public static void main(String[] args) {
        final ZonedDateTime zdt = ZonedDateTime.parse("2016-08-17T05:00Z");
        int size = 10;
        final ListMultiMap<Object, Object> dist = ListMultiMap.create();
        for (int i = 0; i < 60; i++) {
            final ZonedDateTime s = zdt.plusMinutes(i);
            int min = s.getMinute();
            System.out.println("using time: " + s);
            final int p = Math.abs(min + (int) Math.sqrt(min)) % size;
            if (p == 0 && min == 53)
                dist.put(9, min);
            else if (p == 3 && min == 56)
                dist.put(5, min);
            else if(p == 0 && min == 8)
                dist.put(1, min);
            else if(p == 2 && min == 9)
                dist.put(1, min);
            else if(p == 4 && min == 11)
                dist.put(0, min);
            else if(p == 6 && min == 13)
                dist.put(9, min);
            else dist.put(p, min);
        }
        dist.getEntrySet().stream().forEach(e -> System.out.println(e.getKey() + " : " + e.getValue().size()));
        dist.getEntrySet().stream().forEach(e -> System.out.println(e.getKey() + " : " + e.getValue()));
    }*/

    public static void main(String[] args) {
        class X {
            int p;

            public X(int p) {
                this.p = p;
            }
        }
        final PriorityQueue<X> queue = new PriorityQueue<>(((o1, o2) -> o1.p - o2.p));
        queue.add(new X(0));
        queue.add(new X(0));
        queue.add(new X(MAX_VALUE));
        queue.add(new X(100));
        queue.add(new X(1));
        queue.add(new X(100));
        queue.add(new X(0));
        while (!queue.isEmpty())
            System.out.println(queue.poll().p);
    }
}
