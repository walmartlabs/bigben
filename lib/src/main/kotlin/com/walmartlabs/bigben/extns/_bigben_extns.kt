package com.walmartlabs.bigben.extns

import com.walmartlabs.bigben.BigBen.entityProvider
import com.walmartlabs.bigben.entities.Event
import com.walmartlabs.bigben.entities.EventRequest
import com.walmartlabs.bigben.entities.EventResponse
import java.util.*
import java.util.stream.Collectors

fun EventRequest.toResponse() = EventResponse(tenant = tenant, eventTime = eventTime, id = id, mode = mode, payload = payload)
fun Event.toResponse() = eventResponse?.let { it }
        ?: EventResponse(id = xrefId, eventId = id, triggeredAt = processedAt?.toString(),
                tenant = tenant, eventTime = eventTime?.toString(), payload = payload, eventStatus = status)

fun EventResponse.event() = entityProvider<Event>().let { it.raw(it.selector(Event::class.java)) }.also { it.tenant = tenant; it.xrefId = id; triggeredAt = nowUTC().toString(); it.payload = payload; it.eventResponse = this }

fun BitSet.toSet() = stream().boxed().collect(Collectors.toSet())