package com.walmartlabs.components.tests;

import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.MessagePublisher;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.PublisherFactory;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.consumer.KafkaConsumerBean;
import org.testng.annotations.Test;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.walmart.gmp.ingestion.platform.framework.messaging.kafka.Constants.APP_NAME;

/**
 * Created by smalik3 on 9/20/16
 */
public class TestItemServices2 {

    static {
        System.setProperty(APP_NAME, "event-scheduler-app");
        System.setProperty("com.walmart.platform.config.runOnEnv", "prod");
        System.setProperty("min.batch.size", "" + 100);
        System.setProperty("max.batch.size", "" + 1000);
        System.setProperty("max.wait.for.min.batch.size.ms", "3000");
    }

    @Test
    public void testProducer() throws Exception {
        final MessagePublisher<String, String, Object> pub = new PublisherFactory("gmp_bigben_inbound_prod", "IS2BBProps").create();
        int i = 0;
        while (true) {
            i++;
            pub.publish("key_" + i, "value_" + i);
            System.out.println("published: " + i);
            Thread.sleep(300);
        }
    }

    @Test
    public void testConsumer() throws Exception {
        KafkaConsumerBean bean = new KafkaConsumerBean("IS2BBProps", (s, cr) -> {
            System.out.println("got record: " + cr);
            return immediateFuture(null);
        });
        bean.run();
    }

}
