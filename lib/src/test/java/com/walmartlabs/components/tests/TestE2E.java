package com.walmartlabs.components.tests;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.MessagePublisher;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.PublisherFactory;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.TopicMessageProcessor;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.consumer.KafkaConsumerBean;
import com.walmartlabs.components.scheduler.entities.EventRequest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.walmart.gmp.ingestion.platform.framework.messaging.kafka.Constants.APP_NAME;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.setProperty;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.now;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by smalik3 on 7/15/16
 */
@ContextConfiguration(locations = {"/test-beans.xml"/*, "/cluster.xml"*/})
public class TestE2E extends AbstractTestNGSpringContextTests {

    static {
        setProperty("dm.entity.packages.scan", "com.walmartlabs.components.scheduler.entities");
        setProperty("com.walmart.platform.config.runOnEnv", "prod");
        setProperty("event.shard.size", "10");
        setProperty("hazelcast.slow.operation.detector.stacktrace.logging.enabled", "true");
        setProperty("com.walmart.platform.config.appName", "gmp-solr-consumer");
    }

    @Test
    public void produce() throws ExecutionException, InterruptedException {
        setProperty(APP_NAME, "gmp-solr-consumer");
        final MessagePublisher<String, EventRequest, Object> publisher = new PublisherFactory("promo_evt_inbound", "bigben_kafka", true).create();
        final ZonedDateTime now = now(UTC);
        final ZonedDateTime time = now.plusMinutes(5).withSecond(0).withNano(0);
        final int durationSecs = 5 * 60;
        final Random random = new Random();
        final int initial = 1_000_000;
        final Semaphore throttler = new Semaphore(initial);
        final ExecutorService executorService = Executors.newFixedThreadPool(getRuntime().availableProcessors());
        ScheduledExecutorService scheduledThreadPoolExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> throttler.release(initial - throttler.availablePermits()), 0, 1, SECONDS);
        final int size = 1_000_000;
        final CountDownLatch l = new CountDownLatch(size);
        final Map<String, Integer> histogram = new HashMap<>();
        final long n = currentTimeMillis();
        for (int i = 0; i < size; i++) {
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
        System.out.println("done: time to publish " + size + " event = " + (currentTimeMillis() - n) + " ms");
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
            System.out.println(r.value());
            //L.debug("topic: " + s + ", " + r.partition() + ": " + r.offset());
            count.incrementAndGet();
            return immediateFuture(null);
        };
        System.setProperty(APP_NAME, "gmp-solr-consumer");

        final KafkaConsumerBean consumer = new KafkaConsumerBean("bigben_promo_kafka", tmp);
        consumer.afterPropertiesSet();
        new CountDownLatch(1).await();
    }

}
