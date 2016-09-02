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
        System.setProperty("dm.entity.packages.scan", "com.walmart.gmp.ingestion.platform.framework.data.model");
        System.setProperty("com.walmart.platform.config.runOnEnv", "stg0");
    }

    @Autowired
    private ProcessorRegistry processorRegistry;

    @Test
    public void testFeedStatus() throws ExecutionException, InterruptedException, TimeoutException {
        final EventDO eventDO = new EventDO();
        eventDO.setXrefId("3C43604E9FCA489684A2386C19340466@AQQBAAA");
        eventDO.setTenant("GMP/KAFKA/FEED_STATUS/stg0");
        eventDO.setPayload("3C43604E9FCA489684A2386C19340466@AQQBAAA");
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
