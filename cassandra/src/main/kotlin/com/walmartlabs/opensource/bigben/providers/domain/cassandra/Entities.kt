package com.walmartlabs.opensource.bigben.providers.domain.cassandra

import com.walmartlabs.opensource.bigben.entities.*
import java.time.ZonedDateTime

/**
 * Created by smalik3 on 2/26/18
 */
class Bucket(override var id: ZonedDateTime? = null,
             override var status: EventStatus? = null,
             override var count: Long? = null,
             override var processedAt: ZonedDateTime? = null,
             override var updatedAt: ZonedDateTime? = null) : IBucket

class Event(override var id: String? = null,
            override var eventTime: ZonedDateTime? = null,
            override var status: EventStatus? = null,
            override var error: String? = null,
            override var tenant: String? = null,
            override var processedAt: ZonedDateTime? = null,
            override var xRefId: String? = null,
            override var payload: String? = null,
            override var eventResponse: EventResponse? = null) : IEvent

class EventLookup(override var tenant: String?,
                  override var xrefId: String?,
                  override val bucketId: String?,
                  override val shard: Int?,
                  override val eventTime: ZonedDateTime?,
                  override val eventId: String?,
                  override val payload: String?) : IEventLookup
