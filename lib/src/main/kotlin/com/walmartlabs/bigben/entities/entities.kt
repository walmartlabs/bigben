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
package com.walmartlabs.bigben.entities

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.serialization.IdentifiedDataSerializable
import com.walmartlabs.bigben.entities.EventDeliveryOption.FULL_EVENT
import com.walmartlabs.bigben.entities.Mode.UPSERT
import com.walmartlabs.bigben.hz.HzObjectFactory
import com.walmartlabs.bigben.hz.HzObjectFactory.ObjectId.SHARD_STATUS
import com.walmartlabs.bigben.hz.HzObjectFactory.ObjectId.SHARD_STATUS_LIST
import com.walmartlabs.bigben.utils.json
import java.time.Instant.ofEpochMilli
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.time.ZonedDateTime.ofInstant

/**
 * Created by smalik3 on 2/21/18
 */
enum class EventStatus { PROCESSED, ERROR, UN_PROCESSED, PROCESSING, TRIGGERED, EMPTY, REJECTED, ACCEPTED, UPDATED, DELETED }

interface Bucket : IdentifiedDataSerializable {
    var bucketId: ZonedDateTime?
    var status: EventStatus?
    var count: Long?
    var processedAt: ZonedDateTime?
    var updatedAt: ZonedDateTime?
    var failedShards: Set<Int>?
}

interface Event : EventResponseMixin {
    var eventTime: ZonedDateTime?
    var bucketId: ZonedDateTime?
    var shard: Int?
    var id: String?
    var status: EventStatus?
    var error: String?
    var tenant: String?
    var processedAt: ZonedDateTime?
    var xrefId: String?
    var payload: String?
    var deliveryOption: EventDeliveryOption?
}

interface EventLookup {
    var tenant: String?
    var xrefId: String?
    var bucketId: ZonedDateTime?
    var shard: Int?
    var eventTime: ZonedDateTime?
    var eventId: String?
    var payload: String?
}

interface KV {
    var key: String?
    var column: String?
    var value: String?
}

abstract class Idso(private val objectId: HzObjectFactory.ObjectId) : IdentifiedDataSerializable {
    override fun getFactoryId() = HzObjectFactory.BIGBEN_FACTORY_ID
    override fun getId() = objectId.ordinal
}

abstract class IdsoCallable(private val objectId: HzObjectFactory.ObjectId) : IdentifiedDataSerializable {
    override fun getFactoryId() = HzObjectFactory.BIGBEN_FACTORY_ID
    override fun getId() = objectId.ordinal
    override fun writeData(out: ObjectDataOutput?) {
    }

    override fun readData(`in`: ObjectDataInput?) {
    }
}

data class ShardStatus(var bucketId: ZonedDateTime? = null, var shard: Int? = null, var status: EventStatus? = null) : Idso(SHARD_STATUS) {
    override fun writeData(out: ObjectDataOutput) {
        out.writeLong(bucketId!!.toInstant().toEpochMilli())
        out.writeInt(shard!!)
        out.writeByte(status!!.ordinal)
    }

    override fun readData(`in`: ObjectDataInput) {
        bucketId = ofInstant(ofEpochMilli(`in`.readLong()), UTC)
        shard = `in`.readInt()
        status = EventStatus.values()[`in`.readByte().toInt()]
    }
}

data class ShardStatusList(var list: List<ShardStatus?>? = null) : Idso(SHARD_STATUS_LIST) {
    override fun writeData(out: ObjectDataOutput) {
        out.writeInt(list!!.size)
        list!!.forEach { out.writeObject(it) }
    }

    override fun readData(`in`: ObjectDataInput) {
        list = (1..`in`.readInt()).map { `in`.readObject<ShardStatus>() }
    }
}

enum class Mode { UPSERT, REMOVE }
enum class EventDeliveryOption { FULL_EVENT, PAYLOAD_ONLY }
open class EventRequest(
    var id: String? = null, var eventTime: String? = null, var tenant: String? = null,
    var payload: String? = null, var mode: Mode = UPSERT, var deliveryOption: EventDeliveryOption? = FULL_EVENT
) {
    override fun toString() = "EventRequest(${json()})"
}

class EventResponse(
    id: String? = null, eventTime: String? = null, tenant: String? = null, mode: Mode = UPSERT, payload: String? = null,
    var eventId: String? = null, var triggeredAt: String? = null, var eventStatus: EventStatus? = null,
    var error: Error? = null, deliveryOption: EventDeliveryOption? = FULL_EVENT
) :
    EventRequest(id = id, eventTime = eventTime, tenant = tenant, mode = mode, payload = payload, deliveryOption = deliveryOption) {
    override fun toString() = "EventResponse(${json()})"
}

data class Error(val code: Int, val message: String?)

interface EventResponseMixin {
    var eventResponse: EventResponse?
}

