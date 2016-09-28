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

import java.util.Properties;

import static java.util.concurrent.TimeUnit.HOURS;

/**
 * Created by cshah on 9/27/16
 */
@ContextConfiguration(locations = {"/test-beans.xml"})
public class FeedSyncTests extends AbstractTestNGSpringContextTests {

    static {
        String packages = "com.walmartlabs.components.scheduler.entities,com.walmart.gmp.ingestion.platform.framework.data.model.impl,com.walmart.gmp.feeds";
        System.setProperty("dm.entity.packages.scan", packages);
        System.setProperty("com.walmart.platform.config.runOnEnv", "prod");
        System.setProperty("com.walmart.platform.config.appName", "event-scheduler-app");
    }

    @Autowired
    private ProcessorRegistry processorRegistry;

    @Test
    public void testFeedStatus() throws Exception {
        final EventDO eventDO = new EventDO();
        eventDO.setXrefId("40D4A13AA593404BB634AE40F5DF711D@AQkBAAA");
        eventDO.setTenant("GMP/KAFKA/FEED_SYNC");
        Feedstatus feedstatus  = new Feedstatus();
        feedstatus.setFeedType(FeedType.INVENTORY.name());
        feedstatus.setFeedId("40D4A13AA593404BB634AE40F5DF711D@AQkBAAA");
        eventDO.setPayload(JsonUtil.convertToString(feedstatus));
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
