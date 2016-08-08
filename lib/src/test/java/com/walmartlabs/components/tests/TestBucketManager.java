package com.walmartlabs.components.tests;

import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.core.BucketManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.SECONDS;

/*
 * Created by smalik3 on 6/30/16
 */
@ContextConfiguration(locations = {"/test-scheduler.xml"})
public class TestBucketManager extends AbstractTestNGSpringContextTests {

    @Autowired
    private DataManager<?, ?> dm;

    @Test
    public void testBucketManager() {
        //final BucketManager bucketManager = new BucketManager(5, 2 * 60, dm, 1, 2, SECONDS);
        //bucketManager.getProcessableShardsForOrBefore()
    }

}
