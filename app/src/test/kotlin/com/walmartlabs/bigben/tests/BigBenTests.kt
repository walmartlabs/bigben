/*-
 * #%L
 * BigBen:app
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
import com.google.common.util.concurrent.ListeningScheduledExecutorService
import com.sun.net.httpserver.HttpServer
import com.walmartlabs.bigben.BigBen.entityProvider
import com.walmartlabs.bigben.BigBen.eventService
import com.walmartlabs.bigben.BigBen.hz
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.core.BucketManager
import com.walmartlabs.bigben.core.BucketsLoader
import com.walmartlabs.bigben.entities.*
import com.walmartlabs.bigben.entities.EventStatus.*
import com.walmartlabs.bigben.entities.Mode.REMOVE
import com.walmartlabs.bigben.extns.bucket
import com.walmartlabs.bigben.extns.fetch
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.extns.save
import com.walmartlabs.bigben.processors.NoOpCustomClassProcessor
import com.walmartlabs.bigben.processors.ProcessorConfig
import com.walmartlabs.bigben.processors.ProcessorConfig.Type.*
import com.walmartlabs.bigben.utils.commons.Props.int
import com.walmartlabs.bigben.utils.commons.Props.map
import com.walmartlabs.bigben.utils.commons.TaskExecutor
import com.walmartlabs.bigben.utils.fromJson
import com.walmartlabs.bigben.utils.json
import org.apache.commons.text.RandomStringGenerator
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.testng.annotations.BeforeClass
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import java.lang.Thread.sleep
import java.net.InetSocketAddress
import java.time.ZonedDateTime
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit.MINUTES
import java.util.concurrent.atomic.AtomicInteger
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.test.assertEquals
import kotlin.test.assertTrue


/**
 * Created by smalik3 on 4/11/18
 */
class BigBenTests {

    companion object {
        init {
            System.setProperty("props", "file://bigben-test.yaml")
            System.setProperty("org.slf4j.simpleLogger.log.com.walmartlabs.bigben", "debug")
            EventService.DEBUG_FLAG.set(false)
        }
    }

    @BeforeClass
    fun `set up tenant`() {
        `clean up db`()
        eventService.registerProcessor(ProcessorConfig("default", CUSTOM_CLASS,
                mapOf("eventProcessorClass" to NoOpCustomClassProcessor::class.java.name))).apply { assertEquals(this.status, 200) }
        println("tenant set up done")
    }

    @BeforeMethod
    private fun `clean up db`() {
        println("cleaning up the db")
        (entityProvider.unwrap() as Session).apply {
            execute("truncate bigben.events;")
            execute("truncate bigben.lookups;")
            execute("truncate bigben.buckets;")
            execute("truncate bigben.kv_table;")
        }
    }

    @Test
    fun `event service schedule and find API`() {
        val eventTime = nowUTC().plusMinutes(3)
        val tenant = "default"
        val xrefId = "abc"

        //add:
        eventService.schedule(listOf(EventRequest(xrefId, eventTime.toString(), tenant, "P"))).apply {
            assertEquals(status, 200)
        }
        eventService.find(xrefId, tenant).apply {
            assertEquals(status, 200)
            (entity as EventResponse).apply {
                assertEquals(ZonedDateTime.parse(this.eventTime), eventTime)
                assertEquals(payload, "P")
            }
        }

        //update payload:
        eventService.schedule(listOf(EventRequest(xrefId, eventTime.toString(), tenant, "P1"))).apply {
            assertEquals(status, 200)
        }
        eventService.find(xrefId, tenant).apply {
            assertEquals(status, 200)
            (entity as EventResponse).apply {
                assertEquals(ZonedDateTime.parse(this.eventTime), eventTime)
                assertEquals(payload, "P1")
            }
        }

        // update time:
        eventService.schedule(listOf(EventRequest(xrefId, eventTime.plusMinutes(1).toString(), tenant, "P2"))).apply {
            assertEquals(status, 200)
        }
        eventService.find(xrefId, tenant).apply {
            assertEquals(status, 200)
            (entity as EventResponse).apply {
                assertEquals(ZonedDateTime.parse(this.eventTime), eventTime.plusMinutes(1))
                assertEquals(payload, "P2")
            }
        }

        //remove event:
        eventService.schedule(listOf(EventRequest(xrefId, eventTime.plusMinutes(1).toString(), tenant, "P2", REMOVE))).apply {
            assertEquals(status, 200)
        }
        eventService.find(xrefId, tenant).apply {
            assertEquals(status, 404)
        }
    }

    @Test
    fun `sharding works as expected`() {
        val r = Random()
        val time = nowUTC().plusMinutes(2).bucket()
        (0..100).forEach {
            eventService.schedule(listOf(EventRequest("id_$it", time.plusSeconds(r.nextInt(60).toLong()).toString(), "default", "Payload_$it")))
        }
        (0..100).forEach { i ->
            eventService.find("id_$i", "default").apply {
                assertEquals(status, 200)
                fetch<EventLookup> { it.xrefId = "id_$i"; it.tenant = "default" }.get()!!.apply {
                    assertEquals(shard, i / int("events.receiver.shard.size"))
                    fetch<Event> { it.bucketId = time; it.shard = shard; it.eventTime = eventTime; it.id = eventId }.get()!!.apply {
                        assertEquals(status, UN_PROCESSED)
                    }
                }
            }
        }
    }

    @Test
    fun `test bucket loader`() {
        val bucketId = nowUTC().bucket()
        val toBeLoaded = (1..10).map { bucketId.minusMinutes(it.toLong()) }.toSet()

        save<Bucket> { it.bucketId = bucketId.minusMinutes(3); it.count = 100; it.status = PROCESSED }.get()!!

        val latch = CountDownLatch(10)
        val now = System.currentTimeMillis()
        BucketsLoader(10, 5, 60, bucketId) {
            try {
                assertTrue { toBeLoaded.contains(it.bucketId) }
                if (it.bucketId == bucketId.minusMinutes(3)) {
                    assertEquals(it.status, PROCESSED)
                    assertEquals(it.count, 100)
                } else {
                    assertEquals(it.status, EMPTY)
                }
                latch.countDown()
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }.run()
        if (!latch.await(1, MINUTES)) throw IllegalStateException("buckets loader did not complete on time")
        assertTrue { System.currentTimeMillis() - now > 1 }
    }

    @Test
    fun `test bucket manager`() {
        val time = nowUTC().bucket()
        println("time : $time")
        val range = 0..9
        val buckets = range.map { time.minusMinutes(it.toLong()) }.toSortedSet()
        println("buckets: $buckets")
        val shards = range.toList()
        // test back ground load
        range.forEach { i -> save<Bucket> { it.bucketId = time.minusMinutes(i.toLong()); it.count = 100L; it.status = UN_PROCESSED }.get()!! }
        val bm = BucketManager(10, 2 * 60, 60, hz)
        bm.getProcessableShardsForOrBefore(time).get()!!

        sleep(2000)

        bm.getProcessableShardsForOrBefore(time).get()!!.apply {
            assertEquals(this.keySet().toSortedSet(), buckets.toMutableSet().apply { add(time) }.toSortedSet())
            this.keySet().forEach {
                assertEquals(this[it].toList(), shards)
            }
        }
        // test purge:
        (1..5).forEach { i -> save<Bucket> { it.bucketId = time.plusMinutes(i.toLong()); it.count = 100L; it.status = UN_PROCESSED }.get()!! }
        (1..5).forEach { bm.getProcessableShardsForOrBefore(time.plusMinutes(it.toLong())).get()!! }
        bm.purgeIfNeeded()
        (1..5).forEach {
            val b = bm.getProcessableShardsForOrBefore(time.plusMinutes(1)).get()!!
            assertEquals(b.keySet().size, 10)
            assertEquals(buckets - buckets.take(5) + (1..5).map { time.plusMinutes(it.toLong()) }.toSortedSet(), b.keySet().toSortedSet())
        }
    }

    @Test
    fun `test http processor - ok case`() {
        var server: HttpServer? = null
        try {
            val port = 8383
            eventService.registerProcessor(ProcessorConfig("http", HTTP,
                    mapOf
                    (
                            "url" to "http://localhost:$port/test",
                            "headers" to mapOf("header" to "Header1")
                    ))
            ).apply { assertEquals(this.status, 200) }
            val eReq = EventRequest("id123", nowUTC().minusSeconds(1).toString(), "http", "Payload1")
            server = HttpServer.create(InetSocketAddress(port), 0)
            val latch = CountDownLatch(1)
            server.createContext("/test") {
                try {
                    val eResp = EventResponse::class.java.fromJson(String(it.requestBody.readBytes()))
                    assertEquals(it.requestHeaders.getFirst("header"), "Header1")
                    assertEquals(eReq.id, eResp.id)
                    assertEquals(eReq.eventTime, eResp.eventTime)
                    assertEquals(eReq.payload, eResp.payload)
                    assertEquals(eReq.tenant, eResp.tenant)
                    assertEquals(eReq.mode, eResp.mode)
                    assertTrue(eResp.eventId == null)
                    assertTrue(eResp.eventStatus == TRIGGERED)
                    mapOf("status" to "OK").json().apply {
                        it.sendResponseHeaders(200, length.toLong())
                        it.responseBody.write(toByteArray())
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                    mapOf("status" to "error").json().apply {
                        it.sendResponseHeaders(500, length.toLong())
                        it.responseBody.write(toByteArray())
                    }
                } finally {
                    it.close()
                }
                latch.countDown()
            }
            server.start()
            eventService.schedule(listOf(eReq)).apply { assertEquals(200, status) }
            if (!latch.await(1, MINUTES))
                throw AssertionError("latch not down")
        } finally {
            server?.run { stop(0) }
        }
    }

    @Test
    fun `test http processor - error case`() {
        var server: HttpServer? = null
        try {
            val port = 8383
            eventService.registerProcessor(ProcessorConfig("http", HTTP,
                    mapOf
                    (
                            "url" to "http://localhost:$port/test",
                            "header" to "Header1"
                    ))
            ).apply { assertEquals(this.status, 200) }
            val eReq = EventRequest("id123", nowUTC().minusSeconds(1).toString(), "http", "Payload1")
            server = HttpServer.create(InetSocketAddress(port), 0)
            val latch = CountDownLatch(1)
            val tries = AtomicInteger()
            server.createContext("/test") {
                try {
                    tries.incrementAndGet()
                    mapOf("status" to "error").json().apply {
                        it.sendResponseHeaders(500, length.toLong())
                        it.responseBody.write(toByteArray())
                    }
                } finally {
                    it.close()
                }
                latch.countDown()
            }
            server.start()
            eventService.schedule(listOf(eReq)).apply { assertEquals(200, status) }
            if (!latch.await(1, MINUTES))
                throw AssertionError("latch not down")
            var passed = false
            loop@ for (i in (1..10)) {
                if (tries.get() != 4)
                    sleep(1000)
                else {
                    passed = true; break@loop
                }
            }
            assertTrue(passed)
        } finally {
            server?.run { stop(0) }
        }
    }

    @Test
    fun `test http processor - bad request case`() {
        var server: HttpServer? = null
        try {
            val port = 8383
            eventService.registerProcessor(ProcessorConfig("http", HTTP,
                    mapOf
                    (
                            "url" to "http://localhost:$port/test",
                            "header" to "Header1"
                    ))
            ).apply { assertEquals(this.status, 200) }
            val eReq = EventRequest("id123", nowUTC().minusSeconds(1).toString(), "http", "Payload1")
            server = HttpServer.create(InetSocketAddress(port), 0)
            val latch = CountDownLatch(1)
            val tries = AtomicInteger()
            server.createContext("/test") {
                try {
                    tries.incrementAndGet()
                    mapOf("status" to "error").json().apply {
                        it.sendResponseHeaders(400, length.toLong())
                        it.responseBody.write(toByteArray())
                    }
                } finally {
                    it.close()
                }
                latch.countDown()
            }
            server.start()
            eventService.schedule(listOf(eReq)).apply { assertEquals(200, status) }
            if (!latch.await(1, MINUTES))
                throw AssertionError("latch not down")
            var passed = false
            loop@ for (i in (1..5)) {
                if (tries.get() != 1)
                    sleep(1000)
                else {
                    passed = true; break@loop
                }
            }
            sleep(2000)
            assertTrue(passed)
        } finally {
            server?.run { stop(0) }
        }
    }

    @Test
    fun `test kafka integration - ok case`() {
        eventService.registerProcessor(ProcessorConfig("kafka", MESSAGING,
                mapOf
                (
                        "topic" to "test",
                        "brokers.url" to ""
                ))
        ).apply { assertEquals(this.status, 200) }
        val eReq = EventRequest("id123", nowUTC().minusSeconds(1).toString(), "kafka", "Payload1")
        eventService.schedule(listOf(eReq)).apply { assertEquals(200, status) }
    }

    @Test
    fun `test kafka integration - error case`() {
        val x: ScheduledThreadPoolExecutor = (TaskExecutor.Companion::class.memberProperties
                .filter { it.name == "RETRY_POOL" }[0]
                .apply { isAccessible = true }.get(TaskExecutor.Companion) as ListeningScheduledExecutorService)
                .let { it::class.java.getDeclaredField("delegate").apply { isAccessible = true }.get(it) } as ScheduledThreadPoolExecutor
        val current = x.completedTaskCount
        eventService.registerProcessor(ProcessorConfig("kafka", MESSAGING,
                mapOf
                (
                        "topic" to "test",
                        "brokers.url" to "",
                        "fail" to true
                ))
        ).apply { assertEquals(this.status, 200) }
        val eReq = EventRequest("id123", nowUTC().minusSeconds(1).toString(), "kafka", "Payload1")
        eventService.schedule(listOf(eReq)).apply { assertEquals(200, status) }
        sleep(10000)
        assertTrue(x.completedTaskCount - current >= 3.toLong())
    }

    @Test(enabled = false)
    fun `test kafka consumer`() {
        eventService.registerProcessor(ProcessorConfig("kafka", MESSAGING,
                mapOf
                (
                        "topic" to "outbound",
                        "brokers.url" to "localhost:9092"
                ))
        ).apply { assertEquals(this.status, 200) }
        /*val consumer = (BigBen.messageProcessors[0] as MockKafkaProcessor).consumer
        consumer.rebalance(setOf(TopicPartition("inbound", 1)))
        consumer.updateBeginningOffsets(mapOf(TopicPartition("inbound", 0) to 1.toLong()))
        consumer.updateBeginningOffsets(mapOf(TopicPartition("inbound", 0) to Long.MAX_VALUE))*/
        val eReq = EventRequest("id123", nowUTC().minusSeconds(1).toString(), "kafka", "Payload1")
        println(eReq.json())
        //sleep(Long.MAX_VALUE)
    }

    @Test(enabled = false)
    fun `end to end kafka`() {
        eventService.registerProcessor(ProcessorConfig("kafka", MESSAGING,
                mapOf
                (
                        "topic" to "outbound",
                        "bootstrap.servers" to "localhost:9092"
                ))
        ).apply { assertEquals(this.status, 200) }


        val producer = KafkaProducer<String, String>(map("kafka.producer.config").mapKeys { it.key.removePrefix("kafka.producer.config.") } +
                mapOf
                (
                        "topic" to "outbound",
                        "bootstrap.servers" to "localhost:9092"
                ))
        (1..100).forEach {
            println("sending $it")
            val eReq = EventRequest("id123", nowUTC().minusSeconds(1).toString(), "kafka", RandomStringGenerator.Builder().build().generate(1024))
            producer.send(ProducerRecord("outbound", eReq.json())).get()
        }
        sleep(3000)
    }

    @Test(enabled = false)
    fun `test consumer`() {
        val consumer = KafkaConsumer<String, String>(map("kafka.consumer.config") + mapOf("group.id" to UUID.randomUUID().toString()))
        consumer.subscribe(setOf("outbound"))
        while(true) {
            println("polling outbound")
            val records = consumer.poll(3000)
            println(records.count())
            consumer.commitSync()
        }
    }
}
