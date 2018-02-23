package com.walmartlabs.opensource.bigben.entities

import com.walmartlabs.opensource.bigben.entities.Mode.UPSERT
import java.time.ZonedDateTime

/**
 * Created by smalik3 on 2/21/18
 */
enum class EventStatus { PROCESSED, ERROR, UN_PROCESSED, PROCESSING, TRIGGERED, EMPTY, REJECTED, ACCEPTED, UPDATED, DELETED }

data class Bucket(var id: ZonedDateTime? = null, var status: EventStatus? = null, var count: Long? = null, var processedAt: ZonedDateTime? = null, var updatedAt: ZonedDateTime? = null)

data class Event(var status: EventStatus? = null, var error: String? = null, var tenant: String? = null, var processedAt: ZonedDateTime? = null, var xRefId: String? = null, var payload: String? = null)

data class EventLookup(val tenant: String, val xrefId: String, val bucketId: String, val shard: Int, val eventTime: ZonedDateTime, val eventId: String, val payload: String?)

data class ShardStatus(var bucketId: ZonedDateTime? = null, var shard: Int? = null, var status: EventStatus? = null)
data class ShardStatusList(var list: List<ShardStatus?>? = null)

enum class Mode { UPSERT, REMOVE }
open class EventRequest(var id: String? = null, var eventTime: Long? = null, var tenant: String? = null, var mode: Mode = UPSERT)

class EventResponse(id: String? = null, eventTime: Long? = null, tenant: String? = null, mode: Mode = UPSERT,
                    var eventId: String? = null, var triggeredAt: String? = null, var eventStatus: EventStatus? = null) : EventRequest(id, eventTime, tenant, mode)

interface EventResponseMixin {
    var eventResponse: EventResponse
}

