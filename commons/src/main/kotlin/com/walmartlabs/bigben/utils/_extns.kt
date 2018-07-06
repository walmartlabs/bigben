/*-
 * #%L
 * BigBen:commons
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
package com.walmartlabs.bigben.utils

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.common.base.Throwables
import org.slf4j.LoggerFactory
import java.time.ZonedDateTime

/**
 * Created by smalik3 on 2/21/18
 */
inline fun <reified T : Any> logger() = LoggerFactory.getLogger(unwrapCompanionClass(T::class.java).name)!!

fun logger(name: String) = LoggerFactory.getLogger(name)!!

fun <T : Any> unwrapCompanionClass(ofClass: Class<T>): Class<*> {
    return if (ofClass.enclosingClass != null && ofClass.enclosingClass.kotlin.isCompanion) {
        ofClass.enclosingClass
    } else {
        ofClass
    }
}

fun Throwable?.rootCause() = this?.let { Throwables.getRootCause(this) }

fun Throwable?.stackTraceAsString() = this?.let { Throwables.getStackTraceAsString(this) }

fun zdtModule() = SimpleModule("ZDT", Version(1, 0, 0, null, null, null)).also {
    it.addSerializer(ZonedDateTime::class.java, object : JsonSerializer<ZonedDateTime>() {
        override fun serialize(p0: ZonedDateTime?, p1: JsonGenerator, p2: SerializerProvider) {
            p0?.let { p1.writeString(it.toString()) } ?: p1.writeNull()
        }
    })
    it.addDeserializer(ZonedDateTime::class.java, object : JsonDeserializer<ZonedDateTime>() {
        override fun deserialize(jp: JsonParser, dc: DeserializationContext): ZonedDateTime? {
            return jp.codec.readValue(jp, String::class.java)?.let { ZonedDateTime.parse(it) }
        }
    })
}

typealias Json = Map<String, Any>

val om = ObjectMapper().registerModule(KotlinModule()).registerModule(zdtModule())!!

fun Any.json(): String = om.writeValueAsString(this)
fun Any.yaml(): String = omYaml.writeValueAsString(this)
fun <T> Class<T>.fromJson(s: String) = om.readValue(s, this)!!
fun <T> TypeReference<T>.fromJson(s: String): T = om.readValue(s, this)
inline fun <reified T> typeRefJson(s: String) = object : TypeReference<T>() {}.fromJson(s)

val omYaml = ObjectMapper(YAMLFactory()).registerModule(KotlinModule()).registerModule(zdtModule())!!

fun <T> Class<T>.fromYaml(s: String) = omYaml.readValue(s, this)!!
fun <T> TypeReference<T>.fromYaml(s: String): T = omYaml.readValue(s, this)
inline fun <reified T> typeRefYaml(s: String) = object : TypeReference<T>() {}.fromYaml(s)