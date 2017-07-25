package com.walmart.feed.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walmart.gmp.feeds.FeedStatusAndCountsChecker;
import com.walmart.gmp.ingestion.platform.framework.core.FeedType;
import com.walmart.marketplace.messages.v1_bigben.EventRequest;
import com.walmart.marketplace.messages.v1_feedstatus.Feedstatus;
import com.walmart.platform.kernel.exception.layers.service.ServiceException;
import com.walmart.services.common.util.JsonUtil;
import com.walmartlabs.components.scheduler.entities.EventDO;
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;

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
    private  KafkaProducer<String,Object> kafkaProducer;
    @Autowired
    private ProcessorRegistry processorRegistry;


    @Test(enabled = true)
    public void testFeedStatus() throws Exception {
        final EventDO eventDO = new EventDO();
        eventDO.setXrefId("D8129F9D376D48BF9394B2FF2D777E78@AQMBAQA");
        eventDO.setTenant("GMP/KAFKA/FEED_SYNC");
        Feedstatus feedstatus  = new Feedstatus();
        feedstatus.setFeedType(FeedType.ITEM.name());
        feedstatus.setFeedId("D8129F9D376D48BF9394B2FF2D777E78@AQMBAQA");
        eventDO.setPayload(JsonUtil.convertToString(feedstatus));
        processorRegistry.process(eventDO).get(1, HOURS);

        System.out.println("here");
    }

    //{\"feedType\":\"INVENTORY\",\"feedId\":\"E6841B31A23B4097B4898A00A06E88B5@AQkBAAA\",\"cassDMName\":\"inventory\",\"secondaryDMName\":\"inventory_index\",\"additionalProperties\":{}},0

    private String feedId = "552793D31FDD415CBAC5CB138CD45536@AQMBAQA";
    private static final String topic = "gmp_bigben_inbound_prod";

    @Test(enabled = false)
    public void testFeedStatusUpload() throws Exception {
        List<String> listOfFeed  = new ArrayList<>(Arrays.asList("D8129F9D376D48BF9394B2FF2D777E78@AQMBAQA"));

        for (String feedId : listOfFeed) {
            String eventMessage = "";

            EventRequest eventRequest = new EventRequest();
            eventRequest.setPayload(preparePayload(feedId));
            eventRequest.setMode(EventRequest.Mode.ADD);
            eventRequest.setEventTime(Instant.ofEpochMilli(System.currentTimeMillis() + 10000L).toString());
            eventRequest.setTenant("GMP/KAFKA/FEED_STATUS");
            eventRequest.setId(feedId);

            ObjectMapper objectMapper = new ObjectMapper();
            eventMessage = objectMapper.writeValueAsString(eventRequest);
            kafkaProducer.send(new ProducerRecord<>(topic, eventMessage, eventMessage)).get(2, TimeUnit.SECONDS);

            System.out.println("here :" + eventMessage);
        }
    }

    //{"version":"1.0","additionalProperties":{},"messageId":null,"senderId":null,"sentAt":null,"metadata":null,"namespace":null,"name":null,"errors":[],"id":"C0E67F9A0E5D4B6996C9021117128017@AQkBAAA","eventTime":"2016-12-19T05:11:24.312Z","tenant":"GMP/KAFKA/FEED_STATUS","payload":"{\"feedType\":\"INVENTORY\",\"feedId\":\"C0E67F9A0E5D4B6996C9021117128017@AQkBAAA\",\"cassDMName\":\"inventory\",\"secondaryDMName\":\"inventory_index\",\"additionalProperties\":{}}","mode":"ADD"}
    //{"id":"552793D31FDD415CBAC5CB138CD45536@AQMBAQA","eventTime":"2016-12-19T04:19:31.064Z","tenant":"GMP/KAFKA/FEED_STATUS","payload":"{\"feedType\":\"ITEM\",\"feedId\":\"552793D31FDD415CBAC5CB138CD45536@AQMBAQA\",\"cassDMName\":\"item\",\"secondaryDMName\":\"item_index\",\"additionalProperties\":{}}","mode":"ADD","version":"1.0","errors":[]}


    private String preparePayload(String feedId ) throws Exception {
        Feedstatus  feedstatus = new Feedstatus();
        feedstatus.setFeedId(feedId);
        feedstatus.setFeedType("ITEM");
        feedstatus.setCassDMName("item");
        feedstatus.setSecondaryDMName("item_index");
        return JsonUtil.convertToString(feedstatus);
    }

    public static void main(String[] args) throws Exception {
        final String eventProcessorClass = FeedStatusAndCountsChecker.class.getName();
        Properties properties = new Properties();
        final Object o = Class.forName(eventProcessorClass).getConstructor(Properties.class).newInstance(properties);
//        KafkaProducer kafkaProducer = new KafkaProducer<>(getProducerConfig("temp"));
        System.out.println(o);
    }


    @BeforeTest
    private Properties getProducerConfig() {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "dal-kafka-broker06-catdev.prod.walmart.com:9092,dal-kafka-broker07-catdev.prod.walmart.com:9092,dal-kafka-broker08-catdev.prod.walmart.com:9092,dal-kafka-broker09-catdev.prod.walmart.com:9092");
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, properties.getProperty(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"));

        properties.put(ACKS_CONFIG, properties.getProperty(ACKS_CONFIG, "all"));
        properties.put(RETRIES_CONFIG, parseInt(properties.getProperty(RETRIES_CONFIG, "" + 3)));
        properties.put(RETRY_BACKOFF_MS_CONFIG, parseInt(properties.getProperty(RETRY_BACKOFF_MS_CONFIG, "" + 1000)));

        properties.put(METADATA_MAX_AGE_CONFIG, parseInt(properties.getProperty(METADATA_MAX_AGE_CONFIG, "" + 600_000)));
        properties.put(REQUEST_TIMEOUT_MS_CONFIG, parseInt(properties.getProperty(REQUEST_TIMEOUT_MS_CONFIG, "" + 120_000)));
        properties.put(RECONNECT_BACKOFF_MS_CONFIG, parseInt(properties.getProperty(RECONNECT_BACKOFF_MS_CONFIG, "" + 100)));
        //properties.put(PARTITIONER_CLASS_CONFIG, properties.getProperty(PARTITIONER_CLASS_CONFIG, "com.walmart.gmp.ingestion.platform.framework.messaging.kafka.RoundRobinPartitioner"));
        properties.put(COMPRESSION_TYPE_CONFIG, properties.getProperty(COMPRESSION_TYPE_CONFIG, "snappy"));
        kafkaProducer = new KafkaProducer<>(properties);
        return properties;
    }


}
