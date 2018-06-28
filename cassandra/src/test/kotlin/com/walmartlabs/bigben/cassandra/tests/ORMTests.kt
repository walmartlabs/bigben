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

import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.entities.EventStatus.PROCESSED
import com.walmartlabs.bigben.extns.bucket
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.providers.domain.cassandra.BucketC
import com.walmartlabs.bigben.providers.domain.cassandra.CassandraProvider.Companion.mappingManager
import com.walmartlabs.bigben.providers.domain.cassandra.EventC
import com.walmartlabs.bigben.providers.domain.cassandra.EventLookupC
import org.testng.annotations.Test
import java.util.*
import kotlin.test.assertEquals

/**
 * Created by smalik3 on 3/2/18
 */
class ORMTests {

    companion object {
        init {
            System.setProperty("props", "file://bigben-test.yaml")
            System.setProperty("org.slf4j.simpleLogger.log.com.walmartlabs.bigben", "debug")
            EventService.DEBUG_FLAG.set(false)
        }
    }

    @Test
    fun `test bucket orm`() {
        val b = BucketC(nowUTC(), PROCESSED, 10, nowUTC(), nowUTC())
        val mapper = mappingManager.mapper(BucketC::class.java)
        mapper.save(b)
        val newBucket = mapper[b.bucketId]
        assertEquals(b, newBucket)
    }

    @Test
    fun `test event orm`() {
        val eventTime = nowUTC()
        val e = EventC(eventTime, UUID.randomUUID().toString(), eventTime.bucket(), 1, PROCESSED, null,
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
