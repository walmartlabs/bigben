/*-
 * #%L
 * BigBen:cassandra
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
package com.walmartlabs.bigben.cassandra.tests

import com.datastax.driver.core.Session
import com.walmartlabs.bigben.BigBen
import com.walmartlabs.bigben.BigBen.eventLoader
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.entities.Bucket
import com.walmartlabs.bigben.entities.Event
import com.walmartlabs.bigben.entities.EventStatus
import com.walmartlabs.bigben.extns.bucket
import com.walmartlabs.bigben.extns.fetch
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.extns.save
import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Created by smalik3 on 4/12/18
 */
class IntegrationTests {

    companion object {
        init {
            System.setProperty("props", "file://bigben-test.yaml")
            System.setProperty("org.slf4j.simpleLogger.log.com.walmartlabs.bigben", "debug")
            EventService.DEBUG_FLAG.set(false)
        }
    }

    @BeforeClass
    fun `set up`() {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE)
    }

    @AfterClass
    fun `tear down`() {
        EmbeddedCassandraServerHelper.stopEmbeddedCassandra()
    }

    @BeforeMethod
    private fun `clean up db`() {
        println("cleaning up the db")
        (BigBen.entityProvider.unwrap() as Session).apply {
            execute("truncate bigben.events;")
            execute("truncate bigben.lookups;")
            execute("truncate bigben.buckets;")
            execute("truncate bigben.kv_table;")
        }
    }

    @Test
    fun `test bucket`() {
        val nowUTC = nowUTC().bucket()
        save<Bucket> { it.bucketId = nowUTC; it.count = 10 }.get()
        val bucket = fetch<Bucket> { it.bucketId = nowUTC }.get()!!
        assertEquals(bucket.count, 10)
    }

    @Test
    fun `test event loader`() {
        val bucket = nowUTC().bucket()
        val events = (0..99).map { i ->
            save<Event> {
                it.bucketId = bucket; it.shard = i / 10; it.eventTime = bucket.plusSeconds(10)
                it.id = "e_$i"; it.status = EventStatus.UN_PROCESSED
            }.get()
        }.associate { "${it.eventTime}-${it.id}" to it }.toMutableMap()
        val fetchSize = 20
        (0..10).forEach {
            var l = eventLoader.load(bucket, it, fetchSize).get()
            while (l.second.isNotEmpty()) {
                l.second.forEach {
                    assertEquals(events["${it.eventTime}-${it.id}"], it)
                    events.remove("${it.eventTime}-${it.id}")
                }
                l = eventLoader.load(bucket, it, fetchSize, l.second.last().eventTime!!, l.second.last().id!!, l.first).get()
            }
        }
        assertTrue { events.isEmpty() }
    }

    @Test
    fun `event added successfully`() {
        val bucket = nowUTC().bucket()
        save<Event> {
            it.bucketId = bucket; it.shard = 1; it.eventTime = bucket.plusSeconds(10)
            it.id = "e1"; it.status = EventStatus.UN_PROCESSED
        }.get()
        val event = fetch<Event> { it.bucketId = bucket; it.shard = 1; it.eventTime = bucket.plusSeconds(10); it.id = "e1" }.get()!!
        assertEquals(event.status, EventStatus.UN_PROCESSED)
    }
}
