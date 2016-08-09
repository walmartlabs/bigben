package com.walmartlabs.components.tests;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.MessagePublisher;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.PublisherFactory;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.TopicMessageProcessor;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.consumer.KafkaConsumerBean;
import com.walmart.marketplace.messages.v1_bigben.EventRequest;
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.entities.Bucket;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventDO;
import com.walmartlabs.components.scheduler.entities.EventDO.EventKey;
import com.walmartlabs.components.scheduler.input.EventReceiver;
import com.walmartlabs.components.scheduler.processors.EventProcessor;
import com.walmartlabs.components.scheduler.services.BulkEventGeneration;
import com.walmartlabs.components.scheduler.services.EventService;
import info.archinnov.achilles.persistence.AsyncManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static com.walmart.gmp.ingestion.platform.framework.messaging.kafka.Constants.APP_NAME;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.bucketize;
import static java.lang.Integer.parseInt;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.setProperty;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.now;
import static java.time.ZonedDateTime.ofInstant;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by smalik3 on 3/9/16
 */
@ContextConfiguration(locations = {"/test-scheduler.xml"/*, "/cluster.xml"*/})
public class TestEventScheduler extends AbstractTestNGSpringContextTests {

    static {
        setProperty("dm.entity.packages.scan", "com.walmartlabs.components.scheduler.entities");
        setProperty("com.walmart.platform.config.runOnEnv", "prod");
        setProperty("event.shard.size", "10");
        setProperty("hazelcast.slow.operation.detector.stacktrace.logging.enabled", "true");
        setProperty("com.walmart.platform.config.appName", "gmp-solr-consumer");
    }

    @Autowired
    private DataManager<EventKey, Event> dataManager;

    @Autowired
    private DataManager<ZonedDateTime, Bucket> bucketDM;

    @Autowired
    private EventReceiver eventReceiver;

    @Autowired
    private EventService eventService;

    public static class TestEventProcessor implements EventProcessor<Event> {

        @Override
        public ListenableFuture<Event> process(Event event) {
            //System.out.println("processing event: " + event);
            if (counts.decrementAndGet() == 0) {
                System.out.println("test done");
                //latch.countDown();
            }
            return immediateFuture(event);
        }
    }

    private static final CountDownLatch latch = new CountDownLatch(1);
    private static final AtomicInteger counts = new AtomicInteger();

    @Test
    public void testEventScheduler() throws Exception {
        final Integer scanInterval = PROPS.getInteger("event.schedule.scan.interval.minutes", 1);
        final ZonedDateTime now = now(UTC);
        final int delay = 2;
        final long from = bucketize(now.plusMinutes(delay).toInstant().toEpochMilli(), scanInterval);
        final String t1 = ofInstant(ofEpochMilli(from), UTC).toString();
        final int numEvents = 1;
        counts.set(numEvents);
        System.out.println(eventService.generateEvents(new BulkEventGeneration(t1, 1, numEvents, "1")));
        try {
            latch.await(3, MINUTES);
        } catch (InterruptedException e) {
            throw new AssertionError("test timed out");
        }
    }

    @Test
    public void produce() throws ExecutionException, InterruptedException {
        setProperty(APP_NAME, "gmp-solr-consumer");
        final MessagePublisher<String, EventRequest, Object> publisher = new PublisherFactory("promo_evt_inbound", "bigben_kafka", true).create();
        final ZonedDateTime now = now(UTC);
        final ZonedDateTime time = now.plusMinutes(5).withSecond(0).withNano(0);
        final int durationSecs = 60;
        final Random random = new Random();
        final int initial = 1000000;
        final Semaphore throttler = new Semaphore(initial);
        final ExecutorService executorService = Executors.newFixedThreadPool(getRuntime().availableProcessors());
        ScheduledExecutorService scheduledThreadPoolExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> throttler.release(initial - throttler.availablePermits()), 0, 1, SECONDS);
        final int size = 1;
        final CountDownLatch l = new CountDownLatch(size);
        final long n = currentTimeMillis();
        for (int i = 0; i < size; i++) {
            throttler.acquire();
            final ZonedDateTime eventTime = time.plusSeconds(random.nextInt(durationSecs));
            final EventRequest eventRequest = new EventRequest();
            eventRequest.setPayload("Payload" + i);
            eventRequest.setTenant("PROMO/KAFKA/STG");
            eventRequest.setEventTime(eventTime.toString());
            executorService.submit(() -> addCallback(publisher.publish("", eventRequest),
                    new FutureCallback<Object>() {
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
                    }));

        }
        l.await();
        System.out.println("done: time to publish " + size + " event = " + (currentTimeMillis() - n) + " ms");
    }

    @Test
    public void consume() throws Exception {
        final AtomicInteger count = new AtomicInteger();
        final AtomicReference<Integer> sum = new AtomicReference<>(0);
        //bigben_promo_kafka j
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

    @Test
    public void reconcile() throws Exception {
        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(100));
        final Multimap<Integer, Integer> dups = HashMultimap.create();
        final CqlDAO<?, ?> cqlDAO = (CqlDAO<?, ?>) dataManager.getPrimaryDAO(V1).unwrap();
        final AsyncManager am = cqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
        final ZonedDateTime date = ZonedDateTime.parse("2016-07-15T03:20Z");
        final List<ListenableFuture<Multimap<Integer, Integer>>> fs = new ArrayList<>();
        for (int i = 0; i <= 1837; i++) {
            final int j = i;
            final ListenableFuture<Multimap<Integer, Integer>> f = executorService.submit(() -> {
                final Multimap<Integer, Integer> m = HashMultimap.create();
                final List<EventDO> list = am.sliceQuery(EventDO.class).forSelect().withPartitionComponents(date, j).get().get();
                list.forEach(e -> m.put(parseInt(e.getPayload().replace("Payload", "")), j));
                return m;
            });
            fs.add(f);
            addCallback(f, new FutureCallback<Multimap<Integer, Integer>>() {
                @Override
                public void onSuccess(Multimap<Integer, Integer> result) {

                }

                @Override
                public void onFailure(Throwable t) {
                    System.out.println("failed for shard: " + j);
                }
            });

        }
        final List<Multimap<Integer, Integer>> multimaps = successfulAsList(fs).get();
        System.out.println("---------");
        /*final List<Map<Integer, Collection<Integer>>> collect = multimaps.stream().map(Multimap::asMap).collect(Collectors.toList());
        final String x = JsonUtil.convertToString(collect);
        System.out.println(x);
        Files.write(x, new File("/Users/smalik3/dump.txt"), defaultCharset());*/
        System.out.println("---------");
        final Multimap<Integer, Integer> finalMap = HashMultimap.create();
        for (Multimap<Integer, Integer> map : multimaps) {
            finalMap.putAll(map);
            map.keySet().stream().filter(s -> map.get(s).size() > 1).forEach(s -> dups.putAll(s, map.get(s)));
        }
        System.out.println(dups);


        /*final Map<Integer, Integer> payloadCounts = new HashMap<>();
        for (Multimap<Integer, Integer> map : multimaps) {
            for (Integer id : map.keySet()) {
                if (!payloadCounts.containsKey(id))
                    payloadCounts.put(id, 0);
                payloadCounts.put(id, payloadCounts.get(id) + 1);
            }
        }
        payloadCounts.forEach((k, v) -> {
            if (v > 1)
                System.out.println("id = " + k + ", dup = " + v);
        });
        System.out.println("all done");*/
        for (Integer id : finalMap.keySet()) {
            if (finalMap.get(id).size() > 1) {
                System.out.println("id = " + id + ", shards = " + finalMap.get(id));
            }
        }

        //System.out.println(new TreeSet<>(finalMap.keySet()));
        /*final Map<Integer, Integer> counts = new HashMap<>();
        for (Multimap<Integer, Integer> multimap : multimaps) {
            for (Integer id : multimap.keySet()) {
                final Collection<Integer> shards = multimap.get(id);
                for (Integer shard : shards) {
                    if (!counts.containsKey(shard))
                        counts.put(shard, 0);
                    counts.put(shard, counts.get(shard) + 1);
                }
            }
        }
        counts.forEach((k, v) -> {
            System.out.println("shard = " + k + ", count = " + v);
        });*/
        /*final Multimap<Integer, Integer> reverse = TreeMultimap.create();
        for (Integer id : finalMap.keySet()) {
            final Collection<Integer> shards = finalMap.get(id);
            for (Integer shard : shards) {
                reverse.put(shard, id);
            }
        }
        for (Integer shard : reverse.keySet()) {
            System.out.println("shard = " + shard + ", events: " + reverse.get(shard));
        }*/
    }
}
