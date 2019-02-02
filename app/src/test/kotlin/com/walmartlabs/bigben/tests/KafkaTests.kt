/*-
 * #%L
 * BigBen:commons
 * =======================================
 * Copyright (C) 2016 - 2018 Walmart Inc.
 * =======================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.walmartlabs.bigben.tests

import com.datastax.driver.core.Session
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.BigBen
import com.walmartlabs.bigben.BigBen.entityProvider
import com.walmartlabs.bigben.BigBen.module
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.entities.EntityProvider
import com.walmartlabs.bigben.entities.Event
import com.walmartlabs.bigben.entities.EventRequest
import com.walmartlabs.bigben.entities.EventResponse
import com.walmartlabs.bigben.kafka.KafkaMessageProcessor
import com.walmartlabs.bigben.kafka.ProcessorImpl
import com.walmartlabs.bigben.processors.NoOpCustomClassProcessor
import com.walmartlabs.bigben.processors.ProcessorConfig
import com.walmartlabs.bigben.processors.ProcessorConfig.Type.CUSTOM_CLASS
import com.walmartlabs.bigben.processors.ProcessorConfig.Type.MESSAGING
import com.walmartlabs.bigben.processors.ProcessorRegistry
import com.walmartlabs.bigben.utils.commons.PropsLoader
import com.walmartlabs.bigben.utils.fromJson
import com.walmartlabs.bigben.utils.json
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import java.time.ZonedDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by smalik3 on 9/19/18
 */
class KafkaTests {

    companion object {
        val latch = CountDownLatch(1)

        init {
            System.setProperty("bigben.props", "file://bigben-kafka-test.yaml")
            BigBen.init()
        }
    }

    @BeforeMethod
    private fun `clean up db`() {
        println("cleaning up the db")
        try {
            (BigBen.module<EntityProvider<Any>>().unwrap() as Session).apply {
                execute("truncate bigben.events;")
                execute("truncate bigben.lookups;")
                execute("truncate bigben.buckets;")
                execute("truncate bigben.kv_table;")
            }
        } catch (e: Throwable) {
            e.printStackTrace()
            throw e
        }
        println("setting up tenant")
        module<EventService>().registerProcessor(ProcessorConfig("tenant1", CUSTOM_CLASS,
                mapOf("eventProcessorClass" to NoOpCustomClassProcessor::class.java.name)))
    }

    @Test(enabled = false)
    fun `test kafka integration`() {
        println("in kafka tests")

        module<EventService>().registerProcessor(ProcessorConfig("tenant2", MESSAGING,
                mapOf(
                        "topic" to "topic1",
                        "bootstrap.servers" to "localhost:9092"
                )))
        (1..1).forEach {
            entityProvider<Event>().let { it.raw(it.selector(Event::class.java)) }.apply {
                id = "id_$it"
                eventTime = ZonedDateTime.parse("2018-09-19T20:42Z")
                payload = "Payload_$it"
                tenant = "tenant2"
                xrefId = "xref_$id"
            }.apply { module<ProcessorRegistry>().invoke(this).get() }
        }
        if (!latch.await(2, TimeUnit.MINUTES))
            throw AssertionError("test failed")
    }
}

class MockProcessorImpl(props: PropsLoader) : KafkaMessageProcessor(props) {
    private val impl = ProcessorImpl(props)
    private val counter = AtomicInteger(0)

    init {
        println("---> starting the kafka consumer")
        println(EventRequest("id123", "2018-09-19T20:42Z", "tenant1", "Payload1").json())
    }

    override fun process(cr: ConsumerRecord<String, String>): ListenableFuture<Any> {
        println("got a new record: ${cr.value()}")
        val er = EventResponse::class.java.fromJson(cr.value())
        val cr2 = ConsumerRecord(cr.topic(), cr.partition(), cr.offset(), cr.key(), er.run {
            EventRequest(id, eventTime, tenant, payload).json()
        })
        impl.process(cr2).get()
        counter.incrementAndGet()
        if (counter.get() == 10) KafkaTests.latch.countDown()
        return Futures.immediateFuture(cr)
    }
}