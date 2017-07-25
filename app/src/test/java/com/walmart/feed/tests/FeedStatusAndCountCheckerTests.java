package com.walmart.feed.tests;

import com.walmart.gmp.feeds.FeedStatusAndCountsChecker;
import com.walmart.gmp.ingestion.platform.framework.core.FeedType;
import com.walmart.marketplace.messages.v1_feedstatus.Feedstatus;
import com.walmart.services.common.util.JsonUtil;
import com.walmartlabs.components.scheduler.entities.EventDO;
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
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
        String packages = "com.walmartlabs.components.scheduler.entities,com.walmart.gmp.ingestion.platform.framework.data.model.impl,com.walmart.gmp.feeds";
        System.setProperty("dm.entity.packages.scan", packages);
        System.setProperty("com.walmart.platform.config.runOnEnv", "stg");
        System.setProperty("com.walmart.platform.config.appName", "event-scheduler-app");
    }

    final ArrayList<String> arrays = new ArrayList<String>(Arrays.asList("D76FBFEDB9C84B1094DEFD1828F4ACCF@ARQBAAA"));



    @Autowired
    private ProcessorRegistry processorRegistry;

    @Test
    public void testFeedStatus() throws Exception {

        for (String feedId : arrays) {
            final EventDO eventDO = new EventDO();
            eventDO.setXrefId(feedId);
            eventDO.setTenant("GMP/KAFKA/FEED_STATUS/stg0");
            Feedstatus feedstatus = new Feedstatus();
            feedstatus.setFeedType(FeedType.CPT_PRICE.name());
            feedstatus.setFeedId(feedId);
            eventDO.setPayload(JsonUtil.convertToString(feedstatus));
            processorRegistry.process(eventDO).get(2, HOURS);
            System.out.println("done");
        }
    }

    public static void main(String[] args) throws Exception {
        final String eventProcessorClass = FeedStatusAndCountsChecker.class.getName();
        Properties properties = new Properties();
        final Object o = Class.forName(eventProcessorClass).getConstructor(Properties.class).newInstance(properties);
        System.out.println(o);
    }


}
