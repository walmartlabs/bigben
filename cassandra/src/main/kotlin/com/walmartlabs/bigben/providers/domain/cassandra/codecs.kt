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
package com.walmartlabs.bigben.providers.domain.cassandra

import com.datastax.driver.core.DataType
import com.datastax.driver.core.ProtocolVersion
import com.datastax.driver.core.TypeCodec
import com.google.common.reflect.TypeToken
import com.walmartlabs.bigben.extns.utc
import com.walmartlabs.bigben.utils.json
import com.walmartlabs.bigben.utils.typeRefJson
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

class FailedShardsCodec<T> : TypeCodec<Set<Int>>(DataType.text(), object : TypeToken<Set<Int>>() {}) {
    override fun format(value: Set<Int>?) = value?.json()
    override fun parse(value: String?): Set<Int>? = value?.let { typeRefJson<Set<Int>>(it) }

    override fun serialize(value: Set<Int>?, protocolVersion: ProtocolVersion?) = value?.let { ByteBuffer.wrap(it.json().toByteArray()) }
    override fun deserialize(bytes: ByteBuffer?, protocolVersion: ProtocolVersion?) = bytes?.let { typeRefJson<Set<Int>>(String(it.array())) }
}
