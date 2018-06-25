package com.walmartlabs.bigben.entities

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.serialization.IdentifiedDataSerializable
import com.walmartlabs.bigben.entities.Mode.UPSERT
import com.walmartlabs.bigben.hz.HzObjectFactory
import com.walmartlabs.bigben.hz.HzObjectFactory.ObjectId.SHARD_STATUS
import com.walmartlabs.bigben.hz.HzObjectFactory.ObjectId.SHARD_STATUS_LIST
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
open class EventRequest(var id: String? = null, var eventTime: String? = null, var tenant: String? = null, var payload: String? = null, var mode: Mode = UPSERT) {
    override fun toString() = "EventRequest(id=$id, eventTime=$eventTime, tenant=$tenant, payload=$payload, mode=$mode)"
}

class EventResponse(id: String? = null, eventTime: String? = null, tenant: String? = null, mode: Mode = UPSERT, payload: String? = null,
                    var eventId: String? = null, var triggeredAt: String? = null, var eventStatus: EventStatus? = null, var error: Error? = null) :
        EventRequest(id = id, eventTime = eventTime, tenant = tenant, mode = mode, payload = payload) {
    override fun toString() = super.toString() + "EventResponse(eventId=$eventId, triggeredAt=$triggeredAt, eventStatus=$eventStatus, error=$error)"
}

data class Error(val code: Int, val message: String?)

interface EventResponseMixin {
    var eventResponse: EventResponse?
}

