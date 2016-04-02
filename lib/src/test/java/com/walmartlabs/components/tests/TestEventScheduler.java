package com.walmartlabs.components.tests;

import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.core.hz.HzEventReceiver;
import com.walmartlabs.components.scheduler.model.EventBucketStatusEntity;
import com.walmartlabs.components.scheduler.model.EventScheduleDO;
import com.walmartlabs.components.scheduler.model.EventScheduleDO.EventKey;
import com.walmartlabs.components.scheduler.model.EventScheduleEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;
import static java.time.LocalDateTime.now;
import static java.time.ZoneId.systemDefault;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Created by smalik3 on 3/9/16
 */
@ContextConfiguration(locations = "/test-ignite-config.xml")
public class TestEventScheduler extends AbstractTestNGSpringContextTests {

    static {
        System.setProperty("dm.entity.packages.scan", "com.walmartlabs.components.scheduler.model");
        System.setProperty("com.walmart.platform.config.runOnEnv", "dev");
        System.setProperty("ips.list", "file://ips.json");
    }

    @Autowired
    private DataManager<EventKey, EventScheduleEntity> dataManager;

    @Autowired
    private DataManager<Long, EventBucketStatusEntity> bucketDM;

    @Autowired
    private HzEventReceiver eventReceiver;

    @Test
    public void testEventScheduler() throws InterruptedException {
        if (System.getProperty("run.it") != null) {
            System.out.println("loading data");
            final long now = now().withMinute(0).withSecond(0).withNano(0).atZone(systemDefault()).toInstant().toEpochMilli();
            final ExecutorService executorService = new ThreadPoolExecutor(100, 100, 60, MINUTES, new LinkedBlockingQueue<>());
            final AtomicInteger i = new AtomicInteger();
            final Random random = new Random();
            final List<Callable<Object>> tasks = nCopies(10_000, 0L).stream().map($ -> (Callable<Object>) () -> {
                final EventScheduleDO entity = new EventScheduleDO();
                entity.setEventKey(EventKey.of(0, 0, now + random.nextInt(3600) * 1000, "EId#" + i.get() + random.nextInt(1_000_000)));
                eventReceiver.addEvent(entity);
                return null;
            }).collect(Collectors.toList());
            executorService.invokeAll(tasks);
            System.out.println("done: " + (currentTimeMillis() - now) + "ms");
        } else System.out.println("not loading data");
        Thread.sleep(1000000);
    }
}
