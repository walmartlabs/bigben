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
import com.walmartlabs.bigben.BigBen
import com.walmartlabs.bigben.BigBen.entityProvider
import com.walmartlabs.bigben.api.EventReceiver
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.app.App
import com.walmartlabs.bigben.core.BucketManager
import com.walmartlabs.bigben.core.BucketsLoader
import com.walmartlabs.bigben.core.ScheduleScanner
import com.walmartlabs.bigben.entities.*
import com.walmartlabs.bigben.entities.EventStatus.*
import com.walmartlabs.bigben.entities.Mode.REMOVE
import com.walmartlabs.bigben.extns.bucket
import com.walmartlabs.bigben.extns.fetch
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.extns.save
import com.walmartlabs.bigben.providers.domain.cassandra.ClusterConfig
import com.walmartlabs.bigben.utils.hz.Hz
import com.walmartlabs.bigben.utils.json
import com.walmartlabs.bigben.utils.utils.Props
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import java.lang.Thread.*
import java.time.ZonedDateTime
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.HOURS
import java.util.concurrent.TimeUnit.MINUTES
import java.util.function.Predicate
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Created by smalik3 on 4/11/18
 */
class BigBenTests {

    companion object {
        init {
            System.setProperty("org.slf4j.simpleLogger.log.com.walmartlabs.opensource.bigben", "warn")
            System.setProperty("org.slf4j.simpleLogger.log.com.walmartlabs.opensource.bigben.core.BucketManager", "debug")
            //System.setProperty("org.slf4j.simpleLogger.log.com.walmartlabs.opensource.bigben.providers.domain.cassandra", "debug")
            val cc = ClusterConfig()
            cc.contactPoints = "127.0.0.1"
            System.setProperty("bigben.cassandra.config", cc.json())
            System.setProperty("bigben.hz.config", mapOf<String, Any>(
                    "map" to mapOf("store" to mapOf<String, Any>("writeDelay" to 0))).json())
            System.setProperty("skip.tenant.validation", "yes")
            System.setProperty("event.shard.size", 10.toString())
            if (System.getProperty("bigben.cassandra.config") == null) {
                System.setProperty("bigben.cassandra.config", ClusterConfig().apply { contactPoints = "127.0.0.1" }.json())
            }
            System.setProperty("skip.tenant.validation", "yes")
            if (System.getProperty("hz.config") == null) {
                System.setProperty("hz.config", mapOf<String, Any>(
                        "hz" to mapOf("autoIncrement" to true),
                        "map" to mapOf("store" to mapOf<String, Any>("writeDelay" to 10))).json())
            }
            System.setProperty("bigben.entity.provider.class", "com.walmartlabs.bigben.providers.domain.cassandra.CassandraProvider")
        }

        private val hz = Hz()
        private val service = ScheduleScanner(hz)
        private val eventReceiver = EventReceiver(hz)
        val es = EventService(hz, service, eventReceiver)
    }

    @BeforeMethod
    fun `clean up db`() {
        println("cleaning up the db")
        (entityProvider.unwrap() as Session).apply {
            execute("truncate bigben.events;")
            execute("truncate bigben.lookups;")
            execute("truncate bigben.buckets;")
        }
    }

    @Test
    fun `event service schedule and find API`() {
        val eventTime = nowUTC().plusMinutes(3)
        val tenant = "default"
        val xrefId = "abc"

        //add:
        es.schedule(listOf(EventRequest(xrefId, eventTime.toString(), tenant, "P"))).apply {
            assertEquals(status, 200)
        }
        es.find(xrefId, tenant).apply {
            assertEquals(status, 200)
            (entity as EventResponse).apply {
                assertEquals(ZonedDateTime.parse(this.eventTime), eventTime)
                assertEquals(payload, "P")
            }
        }

        //update payload:
        es.schedule(listOf(EventRequest(xrefId, eventTime.toString(), tenant, "P1"))).apply {
            assertEquals(status, 200)
        }
        es.find(xrefId, tenant).apply {
            assertEquals(status, 200)
            (entity as EventResponse).apply {
                assertEquals(ZonedDateTime.parse(this.eventTime), eventTime)
                assertEquals(payload, "P1")
            }
        }

        // update time:
        es.schedule(listOf(EventRequest(xrefId, eventTime.plusMinutes(1).toString(), tenant, "P2"))).apply {
            assertEquals(status, 200)
        }
        es.find(xrefId, tenant).apply {
            assertEquals(status, 200)
            (entity as EventResponse).apply {
                assertEquals(ZonedDateTime.parse(this.eventTime), eventTime.plusMinutes(1))
                assertEquals(payload, "P2")
            }
        }

        //remove event:
        es.schedule(listOf(EventRequest(xrefId, eventTime.plusMinutes(1).toString(), tenant, "P2", REMOVE))).apply {
            assertEquals(status, 200)
        }
        es.find(xrefId, tenant).apply {
            assertEquals(status, 404)
        }
    }

    @Test
    fun `sharding works as expected`() {
        val r = Random()
        val time = nowUTC().plusMinutes(2).bucket()
        (0..100).forEach {
            es.schedule(listOf(EventRequest("id_$it", time.plusSeconds(r.nextInt(60).toLong()).toString(), "default", "Payload_$it")))
        }
        (0..100).forEach { i ->
            es.find("id_$i", "default").apply {
                assertEquals(status, 200)
                fetch<EventLookup> { it.xrefId = "id_$i"; it.tenant = "default" }.get()!!.apply {
                    assertEquals(shard, i / Props.int("event.shard.size"))
                    fetch<Event> { it.bucketId = time; it.shard = shard; it.eventTime = eventTime; it.id = eventId }.get()!!.apply {
                        assertEquals(status, UN_PROCESSED)
                    }
                }
            }
        }
    }

    @Test
    fun `test bucket loader`() {
        System.setProperty("buckets.background.load.wait.interval.seconds", 1.toString())
        val bucketId = nowUTC().bucket()
        val toBeLoaded = (1..10).map { bucketId.minusMinutes(it.toLong()) }.toSet()

        save<Bucket> { it.bucketId = bucketId.minusMinutes(3); it.count = 100; it.status = PROCESSED }.get()!!

        val latch = CountDownLatch(10)
        val now = System.currentTimeMillis()
        BucketsLoader(10, 5, Predicate { false }, 60, bucketId) {
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
        val bm = BucketManager(10, 2 * 60, 60, 1, HOURS, BigBen.hz)
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
    fun `start app`() {
        Executors.newCachedThreadPool().apply {
            submit { currentThread().isDaemon = true; App() }
            //submit { currentThread().isDaemon = true; App() }
        }
        Thread.sleep(Long.MAX_VALUE)
    }
}
