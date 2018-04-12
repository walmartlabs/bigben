package com.walmartlabs.opensource.core

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.common.base.Throwables
import org.slf4j.LoggerFactory
import java.time.ZonedDateTime
import java.util.*

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

private val om = ObjectMapper().registerModule(KotlinModule()).apply {
    SimpleModule("ZDT", Version(1, 0, 0, null, null, null)).also {
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
        registerModule(it)
    }
}

fun Any.json(): String = om.writeValueAsString(this)
fun <T> Class<T>.fromJson(s: String) = om.readValue(s, this)!!
fun <T> TypeReference<T>.fromJson(s: String): T = om.readValue(s, this)
inline fun <reified T> typeRefJson(s: String) = object : TypeReference<T>() {}.fromJson(s)

inline fun <reified T> createProvider(): T {
    return try {
        ServiceLoader.load(T::class.java).first()!!.also {
            if (_l.isInfoEnabled) _l.info("provider {} loaded successfully", T::class.java.name)
        }
    } catch (e: Throwable) {
        _l.error("could not load provider {}", T::class.java.name, e.rootCause())
        throw ExceptionInInitializerError(e.rootCause())
    }
}
