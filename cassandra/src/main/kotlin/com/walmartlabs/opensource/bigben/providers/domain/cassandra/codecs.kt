package com.walmartlabs.opensource.bigben.providers.domain.cassandra

import com.datastax.driver.core.DataType
import com.datastax.driver.core.ProtocolVersion
import com.datastax.driver.core.TypeCodec
import com.walmartlabs.opensource.bigben.extns.utc
import java.nio.ByteBuffer
import java.time.ZonedDateTime

/**
 * Created by smalik3 on 3/2/18
 */
class EnumCodec<T : Enum<T>>(values: Set<T>) : TypeCodec<T>(DataType.varchar(), @Suppress("UNCHECKED_CAST") (values.first()::class.java as Class<T>)) {
    private val forward = values.associate { it.name to it }

    override fun format(value: T) = value.name
    override fun parse(value: String?) = value?.let { forward[it] }

    override fun serialize(value: T?, protocolVersion: ProtocolVersion?) = value?.let { ByteBuffer.wrap(format(it).toByteArray()) }
    override fun deserialize(bytes: ByteBuffer?, protocolVersion: ProtocolVersion?) = bytes?.let { parse(String(bytes.duplicate().array())) }
}

class ZdtCodec : TypeCodec<ZonedDateTime>(DataType.timestamp(), ZonedDateTime::class.java) {
    override fun format(value: ZonedDateTime?) = value?.toInstant()?.toEpochMilli()?.toString()
    override fun parse(value: String?): ZonedDateTime? = value?.let { utc(it.toLong()) }

    override fun serialize(value: ZonedDateTime?, protocolVersion: ProtocolVersion?) = value?.let { ByteBuffer.allocate(8).apply { asLongBuffer().put(value.toInstant().toEpochMilli()) } }
    override fun deserialize(bytes: ByteBuffer?, protocolVersion: ProtocolVersion?) = bytes?.let { utc(bytes.duplicate().asLongBuffer().get()) }
}