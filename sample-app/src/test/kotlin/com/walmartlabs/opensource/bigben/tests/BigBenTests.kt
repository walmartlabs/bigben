package com.walmartlabs.opensource.bigben.tests

import com.datastax.driver.core.Session
import com.walmartlabs.opensource.bigben.api.EventReceiver
import com.walmartlabs.opensource.bigben.api.EventService
import com.walmartlabs.opensource.bigben.core.ScheduleScanner
import com.walmartlabs.opensource.bigben.entities.*
import com.walmartlabs.opensource.bigben.entities.EventStatus.UN_PROCESSED
import com.walmartlabs.opensource.bigben.entities.Mode.REMOVE
import com.walmartlabs.opensource.bigben.extns.bucket
import com.walmartlabs.opensource.bigben.extns.domainProvider
import com.walmartlabs.opensource.bigben.extns.fetch
import com.walmartlabs.opensource.bigben.extns.nowUTC
import com.walmartlabs.opensource.bigben.providers.domain.cassandra.ClusterConfig
import com.walmartlabs.opensource.bigben.tasks.BulkShardTask
import com.walmartlabs.opensource.core.hz.ClusterSingleton
import com.walmartlabs.opensource.core.hz.Hz
import com.walmartlabs.opensource.core.json
import com.walmartlabs.opensource.core.utils.Props
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import java.time.ZonedDateTime
import java.util.*
import kotlin.test.assertEquals

/**
 * Created by smalik3 on 4/11/18
 */
class BigBenTests {

    companion object {
        init {
            val cc = ClusterConfig()
            cc.contactPoints = "127.0.0.1"
            System.setProperty("bigben.cassandra.config", cc.json())
            System.setProperty("bigben.hz.config", mapOf<String, Any>(
                    "map" to mapOf("store" to mapOf<String, Any>("writeDelay" to 0))).json())
            System.setProperty("skip.tenant.validation", "yes")
            System.setProperty("event.shard.size", 10.toString())
        }

        val loader = (BulkShardTask::class.java.getDeclaredField("loader").apply { isAccessible = true }.get(null) as EventLoader)

        private val hz = Hz()
        private val service = ScheduleScanner(hz)
        private val cs = ClusterSingleton(service, hz)
        private val eventReceiver = EventReceiver(hz)
        val es = EventService(hz, service, eventReceiver)
    }

    @BeforeMethod
    fun `clean up db`() {
        println("cleaning up the db")
        (domainProvider.unwrap() as Session).apply {
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
}