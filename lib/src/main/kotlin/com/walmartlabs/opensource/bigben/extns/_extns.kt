package com.walmartlabs.opensource.bigben.extns

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Throwables
import org.slf4j.LoggerFactory

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

private val om = ObjectMapper()
fun Any.json(): String = om.writeValueAsString(this)
fun <T> Class<T>.fromJson(s: String) = om.readValue(s, this)
fun <T> TypeReference<T>.fromJson(s: String): T = om.readValue(s, this)
