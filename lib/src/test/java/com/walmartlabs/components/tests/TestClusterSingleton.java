package com.walmartlabs.components.tests;

import com.walmartlabs.components.scheduler.core.Service;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

/**
 * Created by smalik3 on 6/13/16
 */
@ContextConfiguration(locations = "/test-scheduler-beans.xml")
public class TestClusterSingleton extends AbstractTestNGSpringContextTests {

    public static class TestService implements Service {

        @Override
        public String name() {
            return "TestService";
        }

        @Override
        public void init() {

        }

        @Override
        public void execute() {

        }

        @Override
        public void destroy() {

        }
    }

    @Test
    public void test() throws InterruptedException {
        System.out.println("here");
        new CountDownLatch(1).await();
    }

}
