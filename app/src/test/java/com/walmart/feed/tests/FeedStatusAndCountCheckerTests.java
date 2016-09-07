package com.walmart.feed.tests;

import com.walmart.gmp.feeds.FeedStatusAndCountsChecker;
import com.walmartlabs.components.scheduler.entities.EventDO;
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.HOURS;

/**
 * Created by smalik3 on 8/26/16˙
 */
@ContextConfiguration(locations = {"/test-beans.xml"})
public class FeedStatusAndCountCheckerTests extends AbstractTestNGSpringContextTests {

    static {
        String packages = "com.walmartlabs.components.scheduler.entities,com.walmart.gmp.ingestion.platform.framework.data.model.impl";
        System.setProperty("dm.entity.packages.scan", packages);
        System.setProperty("com.walmart.platform.config.runOnEnv", "stg0");
        System.setProperty("com.walmart.platform.config.appName", "event-scheduler-app");
    }

    @Autowired
    private ProcessorRegistry processorRegistry;

    @Test
    public void testFeedStatus() throws ExecutionException, InterruptedException, TimeoutException {
        final EventDO eventDO = new EventDO();
        eventDO.setXrefId("4ED9379B92CF4EF7B5974594BE2FEE82@AQYBAAA");
        eventDO.setTenant("GMP/KAFKA/FEED_STATUS/stg0");
        eventDO.setPayload("4ED9379B92CF4EF7B5974594BE2FEE82@AQYBAAA");
        processorRegistry.process(eventDO).get(2, HOURS);
        System.out.println("here");
    }

    public static void main(String[] args) throws Exception {
        final String eventProcessorClass = FeedStatusAndCountsChecker.class.getName();
        Properties properties = new Properties();
        final Object o = Class.forName(eventProcessorClass).getConstructor(Properties.class).newInstance(properties);
        System.out.println(o);
    }


}
