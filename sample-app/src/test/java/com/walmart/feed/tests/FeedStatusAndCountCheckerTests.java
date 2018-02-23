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

import java.io.IOException;
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
        System.setProperty("ccmProps", "bigbenProps");

    }

    @Autowired
    private ProcessorRegistry processorRegistry;

    @Test
    public void testFeedStatus() throws ExecutionException, InterruptedException, TimeoutException, IOException {
        final EventDO eventDO = new EventDO();
        eventDO.setXrefId("95FE76AD3A894F9884767CCD9D4B0E84@ARgBAAA");
        eventDO.setTenant("GMP/KAFKA/FEED_STATUS");
        Feedstatus feedstatus  = new Feedstatus();
        feedstatus.setFeedType(FeedType.SUPPLIER_FULL_ITEM.name());
        feedstatus.setFeedId("95FE76AD3A894F9884767CCD9D4B0E84@ARgBAAA");
        eventDO.setPayload(JsonUtil.convertToString(feedstatus));
        processorRegistry.process(eventDO).get(2, HOURS);
        System.out.println("here");
        Thread.sleep(5000l);
    }

    public static void main(String[] args) throws Exception {
        final String eventProcessorClass = FeedStatusAndCountsChecker.class.getName();
        Properties properties = new Properties();
        final Object o = Class.forName(eventProcessorClass).getConstructor(Properties.class).newInstance(properties);
        System.out.println(o);
    }


}
