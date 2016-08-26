package com.walmart.feed.tests;

import com.walmart.gmp.feeds.FeedStatusAndCountsChecker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Created by smalik3 on 8/26/16
 */
@ContextConfiguration(locations = {"/test-beans.xml"})
public class FeedStatusAndCountCheckerTests extends AbstractTestNGSpringContextTests {

    static {
        System.setProperty("dm.entity.packages.scan", "com.walmart.gmp.ingestion.platform.framework.data.model.impl.v2.item");
    }

    @Autowired
    private FeedStatusAndCountsChecker checker;

    @Test
    public void testFeedStatus() {
        System.out.println("here");
    }


}
