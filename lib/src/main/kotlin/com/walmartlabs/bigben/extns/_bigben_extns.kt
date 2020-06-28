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
import com.walmartlabs.bigben.entities.Error
import com.walmartlabs.bigben.entities.Event
import com.walmartlabs.bigben.entities.EventDeliveryOption
import com.walmartlabs.bigben.entities.EventDeliveryOption.FULL_EVENT
import com.walmartlabs.bigben.entities.EventDeliveryOption.PAYLOAD_ONLY
import com.walmartlabs.bigben.entities.EventRequest
import com.walmartlabs.bigben.entities.EventResponse
import java.time.ZonedDateTime
import java.util.*
import java.util.UUID.randomUUID
import java.util.stream.Collectors

fun EventRequest.toResponse() = EventResponse(
    tenant = tenant, eventTime = eventTime, id = id,
    mode = mode, payload = payload, deliveryOption = deliveryOption
)

fun Event.toResponse() = eventResponse?.let { it }
    ?: EventResponse(
        id = xrefId, eventId = id, triggeredAt = processedAt?.toString(),
        tenant = tenant, eventTime = eventTime?.toString(), payload = payload,
        eventStatus = status, deliveryOption = deliveryOption(this), error = Error(-1, error)
    )

fun EventResponse.event() = entityProvider<Event>().let { it.raw(it.selector(Event::class.java)) }.also {
    val t = ZonedDateTime.parse(triggeredAt)
    it.tenant = tenant; it.xrefId = id; it.eventTime = ZonedDateTime.parse(eventTime)!!; it.payload = payload
    it.id = eventId; it.bucketId = t.bucket(); it.processedAt = t
    if (eventId == null) it.deliveryOption = deliveryOption
    it.error = error?.message
}

fun BitSet.toSet(): MutableSet<Int> = stream().boxed().collect(Collectors.toSet())!!

fun eventId(req: EventRequest) = if (req.deliveryOption == FULL_EVENT) randomUUID().toString() else "a-${randomUUID()}"

fun deliveryOption(event: Event): EventDeliveryOption? {
    return when {
        event.id != null -> if (event.id!!.startsWith("a-")) PAYLOAD_ONLY else FULL_EVENT
        event.deliveryOption != null -> event.deliveryOption
        else -> null
    }
}
