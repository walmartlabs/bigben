/*-
 * #%L
 * BigBen:lib
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
package com.walmartlabs.bigben.extns

import com.walmartlabs.bigben.BigBen.entityProvider
import com.walmartlabs.bigben.entities.Event
import com.walmartlabs.bigben.entities.EventRequest
import com.walmartlabs.bigben.entities.EventResponse
import java.time.ZonedDateTime
import java.util.*
import java.util.stream.Collectors

fun EventRequest.toResponse() = EventResponse(tenant = tenant, eventTime = eventTime, id = id, mode = mode, payload = payload)
fun Event.toResponse() = eventResponse?.let { it }
        ?: EventResponse(id = xrefId, eventId = id, triggeredAt = processedAt?.toString(),
                tenant = tenant, eventTime = eventTime?.toString(), payload = payload, eventStatus = status)

fun EventResponse.event() = entityProvider<Event>().let { it.raw(it.selector(Event::class.java)) }.also {
    val t = ZonedDateTime.parse(triggeredAt)
    it.tenant = tenant; it.xrefId = id; it.eventTime = ZonedDateTime.parse(eventTime)!!; it.payload = payload
    it.id = eventId; it.bucketId = t.bucket(); it.processedAt = t
}

fun BitSet.toSet(): MutableSet<Int> = stream().boxed().collect(Collectors.toSet())!!
