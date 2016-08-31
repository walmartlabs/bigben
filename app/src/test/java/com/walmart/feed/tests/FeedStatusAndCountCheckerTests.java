package com.walmart.feed.tests;

import com.walmart.gmp.feeds.FeedStatusAndCountsChecker;
import com.walmartlabs.components.scheduler.entities.EventDO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

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
    private FeedStatusAndCountsChecker checker;

    @Test
    public void testFeedStatus() throws ExecutionException, InterruptedException {
        final EventDO eventDO = new EventDO();
        eventDO.setXrefId("BFBABBACA91A466998D7C17564496060@AQQBAAA");
        checker.process(eventDO).get();
        System.out.println("here");
    }


}
