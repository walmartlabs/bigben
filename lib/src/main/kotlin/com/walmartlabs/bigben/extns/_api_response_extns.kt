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
package com.walmartlabs.bigben.extns

import com.walmartlabs.bigben.BigBen.module
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.utils.hz.Hz
import com.walmartlabs.bigben.utils.json
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.rootCause
import com.walmartlabs.bigben.utils.stackTraceAsString
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeParseException
import java.util.*

/**
 * Created by smalik3 on 6/29/18
 */
private fun String.base64() = Base64.getEncoder().encodeToString(this.json().toByteArray())

private val l = logger("API")

data class APIResponse(
    val entity: Any, val status: Int = 200,
    val headers: Map<String, MutableList<String>> = mutableMapOf()
) {
    fun header(name: String, value: String) = apply { (headers[name] ?: mutableListOf()).add(value) }
}

fun response(f: () -> Any?): APIResponse {
    val begin = LocalDateTime.now()
    val r = try {
        f()?.run { this as? APIResponse ?: APIResponse(this, 200) }
            ?: APIResponse(mapOf("status" to "not found"), 404)
    } catch (e: Exception) {
        val t = e.rootCause()!!
        l.error("error in processing request", t)
        val status = if (t is IllegalArgumentException || t is DateTimeParseException) 400 else 500
        val message = "please contact engineering team with the below error signature"
        APIResponse(
            mutableMapOf("message"
                    to (t.message?.let { """${t.message}${if (status == 500) " ($message)" else ""}""" }
                ?: "Unexpected error, $message")).apply {
                if (status == 500) {
                    this["error"] = mapOf(
                        "stack" to t.stackTraceAsString()!!,
                        "node" to module<Hz>().hz.cluster.localMember.address.host,
                        "start_time" to begin,
                        "duration" to (LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli() - begin.toInstant(ZoneOffset.UTC).toEpochMilli())
                    ).json().run { if (EventService.DEBUG_FLAG.get()) this else base64() }
                }
            }, status
        )
    }
    val end = LocalDateTime.now()
    r.header("Start-Time", begin.toString()).header("End-Time", end.toString())
        .header("Duration", "${end.toInstant(ZoneOffset.UTC).toEpochMilli() - begin.toInstant(ZoneOffset.UTC).toEpochMilli()} ms")
        .header("Node", module<Hz>().hz.cluster.localMember.address.host)
    return r
}
