package com.walmartlabs.bigben.cassandra.tests

import com.walmartlabs.bigben.api.EventReceiver
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.core.ScheduleScanner
import com.walmartlabs.bigben.entities.*
import com.walmartlabs.bigben.extns.bucket
import com.walmartlabs.bigben.extns.fetch
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.extns.save
import com.walmartlabs.bigben.tasks.BulkShardTask
import com.walmartlabs.bigben.utils.hz.ClusterSingleton
import com.walmartlabs.bigben.utils.hz.Hz
import com.walmartlabs.bigben.utils.json
import org.testng.annotations.Test
import java.time.ZonedDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Created by smalik3 on 4/12/18
 */
class IntegrationTests {

    companion object {
        init {
            System.setProperty("bigben.hz.config", mapOf<String, Any>(
                    "map" to mapOf("store" to mapOf<String, Any>("writeDelay" to 0))).json())
            System.setProperty("skip.tenant.validation", "yes")
        }

        val loader = (BulkShardTask::class.java.getDeclaredField("loader").apply { isAccessible = true }.get(null) as EventLoader)

        private val hz = Hz()
        private val service = ScheduleScanner(hz)
        private val cs = ClusterSingleton(service, hz)
        private val eventReceiver = EventReceiver(hz)
        val es = EventService(hz, service, eventReceiver)
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
            var l = loader.load(bucket, it, fetchSize).get()
            while (l.second.isNotEmpty()) {
                l.second.forEach {
                    assertEquals(events["${it.eventTime}-${it.id}"], it)
                    events.remove("${it.eventTime}-${it.id}")
                }
                l = loader.load(bucket, it, fetchSize, l.second.last().eventTime!!, l.second.last().id!!, l.first).get()
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

    @Test
    fun `event service schedule API`() {
        val eventTime = nowUTC().plusMinutes(3)
        val tenant = "default"
        val xrefId = "abc"

        //add:
        es.schedule(listOf(EventRequest(xrefId, eventTime.toString(), tenant, "P"))).apply {
            assertEquals(status, 200)
        }
        val l = fetch<EventLookup> { it.tenant = tenant; it.xrefId = xrefId }.get()!!.apply {
            assertEquals(payload, "P")
        }
        fetch<Event> { it.bucketId = eventTime.bucket(); it.shard = l.shard; it.eventTime = eventTime; it.id = l.eventId }.get()!!.apply {
            assertEquals(status, EventStatus.UN_PROCESSED)
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
        fetch<EventLookup> { it.tenant = tenant; it.xrefId = xrefId }.get()!!.apply {
            assertEquals(payload, "P1")
        }
        fetch<Event> { it.bucketId = eventTime.bucket(); it.shard = l.shard; it.eventTime = eventTime; it.id = l.eventId }.get()!!.apply {
            assertEquals(status, EventStatus.UN_PROCESSED)
        }
        fetch<Bucket> { it.bucketId = eventTime.bucket() }.get()?.let {
            assertNotNull(it)
            assertTrue { it.count!! > 0L }
        }
        es.find(xrefId, tenant).apply {
            assertEquals(status, 200)
            (entity as EventResponse).apply {
                assertEquals(ZonedDateTime.parse(this.eventTime), eventTime)
                assertEquals(payload, "P1")
            }
        }
    }

}