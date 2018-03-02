package com.walmartlabs.opensource.bigben.cassandra.tests

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.CodecRegistry
import com.datastax.driver.mapping.MappingManager
import com.walmartlabs.opensource.bigben.entities.EventStatus
import com.walmartlabs.opensource.bigben.extns.bucket
import com.walmartlabs.opensource.bigben.extns.nowUTC
import com.walmartlabs.opensource.bigben.providers.domain.cassandra.*
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import java.util.*
import kotlin.test.assertEquals

/**
 * Created by smalik3 on 3/2/18
 */
class BucketTests {

    private lateinit var cluster: Cluster
    private lateinit var mappingManager: MappingManager

    @BeforeClass
    fun `set up cluster`() {
        cluster = Cluster.builder()
                .withClusterName("bigben_test_cluster")
                .addContactPoint("127.0.0.1")
                .withCodecRegistry(CodecRegistry().
                        register(EnumCodec(EventStatus.values().toSet())).
                        register(ZdtCodec())
                )
                .build()
        mappingManager = MappingManager(cluster.connect())
    }

    @Test
    fun `test bucket orm`() {
        val b = BucketC(nowUTC(), EventStatus.PROCESSED, 10, nowUTC(), nowUTC())
        val mapper = mappingManager.mapper(BucketC::class.java)
        mapper.save(b)
        val newBucket = mapper[b.id]
        assertEquals(b, newBucket)
    }

    @Test
    fun `test event orm`() {
        val eventTime = nowUTC()
        val e = EventC(eventTime, UUID.randomUUID().toString(), eventTime.bucket(), 1, EventStatus.PROCESSED, null,
                "default", processedAt = eventTime.plusSeconds(1), xrefId = "xref_1", payload = "{payload}")
        val mapper = mappingManager.mapper(EventC::class.java)
        mapper.save(e)
        val newEventC = mapper[eventTime.bucket(), 1, eventTime, e.id]
        assertEquals(e, newEventC)
    }

    @Test
    fun `test event look up orm`() {
        val el = EventLookupC("default", UUID.randomUUID().toString(), nowUTC().bucket(), 2, nowUTC(), "event_1", "payload1")
        val mapper = mappingManager.mapper(EventLookupC::class.java)
        mapper.save(el)
        val newEventLookupC = mapper[el.tenant, el.xrefId]
        assertEquals(el, newEventLookupC)
    }
}