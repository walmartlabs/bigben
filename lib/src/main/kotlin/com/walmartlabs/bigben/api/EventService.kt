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
package com.walmartlabs.bigben.api

import com.google.common.base.Throwables.getStackTraceAsString
import com.walmartlabs.bigben.BigBen.module
import com.walmartlabs.bigben.entities.*
import com.walmartlabs.bigben.entities.EventStatus.*
import com.walmartlabs.bigben.extns.*
import com.walmartlabs.bigben.processors.ProcessorConfig
import com.walmartlabs.bigben.processors.ProcessorRegistry
import com.walmartlabs.bigben.tasks.StatusTask
import com.walmartlabs.bigben.utils.*
import com.walmartlabs.bigben.utils.hz.Hz
import com.walmartlabs.bigben.utils.hz.Service
import java.io.Serializable
import java.util.concurrent.Callable

/**
 * Created by smalik3 on 2/27/18
 */
class EventService(
    private val hz: Hz, private val service: Service,
    private val receiver: EventReceiver
) {

    companion object {
        private val l = logger<EventService>()
        val DEBUG_FLAG = ThreadLocal<Boolean>()
    }

    fun clusterStats() = response {
        hz.hz.getExecutorService("default").submitToAllMembers(StatusTask(service.name))
            .mapKeys { it.key.address.toString() }
            .mapValues { it.value.result { "Error: ${getStackTraceAsString(it)}" } }
    }

    fun schedule(events: List<EventRequest>) = response {
        events.map { if (it.mode == Mode.UPSERT) receiver.addEvent(it) else receiver.removeEvent(it.id!!, it.tenant!!) }
            .reduce().result { emptyList() }.run {
                filter { it.eventStatus == TRIGGERED }.map { module<ProcessorRegistry>()(it.event()) }
                    .done({ l.error("error in triggering lapsed events:", it.rootCause()) }) {
                        it!!.forEach {
                            l.warn(
                                "event was triggered immediately (likely lapsed), event bucketId: {}, tenant: {}, " +
                                        "eventTime: {}, currentTime: {}", it.xrefId, it.tenant, it.eventTime, nowUTC()
                            )
                        }
                    }
                count { it.eventStatus == REJECTED }.let {
                    when {
                        it == events.size -> APIResponse(this, 400)
                        it > 0 -> APIResponse(this, 206)
                        else -> APIResponse(this)
                    }
                }
            }
    }

    fun registerProcessor(config: ProcessorConfig) = response {
        if (l.isInfoEnabled) l.info("saving the tenant config: $config")
        require(config.tenant != null) { "tenant must not be null" }
        save<KV> { it.key = "tenants"; it.column = config.tenant; it.value = config.json() }
        if (l.isInfoEnabled) l.info("broadcasting the tenant config to all members: $config")
        hz.hz.getExecutorService("default").submitToAllMembers(ProcessRegisterTask(config))
            .mapValues { it.value.listenable() }.values.toList().reduce().result { throw RuntimeException("") }
        module<ProcessorRegistry>().registeredConfigs()
    }

    fun registeredTenants() = response { module<ProcessorRegistry>().registeredConfigs() }

    fun find(id: String, tenant: String) = response {
        find(EventRequest().apply {
            this.id = id; this.tenant = tenant
        }, false)
    }

    fun dryrun(id: String, tenant: String) = response {
        find(EventRequest().apply {
            this.id = id; this.tenant = tenant
        }, true)
    }

    private fun find(eventRequest: EventRequest, fire: Boolean): EventResponse? {
        val eventResponse = eventRequest.toResponse()
        return if (eventRequest.id != null && eventRequest.id!!.trim().isNotEmpty()) {
            fetch<EventLookup> { it.xrefId = eventRequest.id; it.tenant = eventRequest.tenant }.result { null }
                ?.let { el ->
                    fetch<Event> {
                        it.id = el.eventId; it.eventTime = el.eventTime; it.shard = el.shard; it.bucketId = el.bucketId
                    }.result { null }?.run {
                        eventResponse.also {
                            it.eventId = id; it.eventTime = eventTime?.toString(); it.payload = payload
                            it.eventStatus = status; if (status != UN_PROCESSED && status != null) it.triggeredAt =
                                processedAt?.toString()
                            if (error != null) it.error = com.walmartlabs.bigben.entities.Error(500, error)
                        }.also { if (fire) module<ProcessorRegistry>()(this) }
                    }
                }
        } else {
            throw IllegalArgumentException("null id")
        }
    }

    class ProcessRegisterTask(private val config: ProcessorConfig) : Serializable, Callable<ProcessorConfig?> {
        override fun call() = module<ProcessorRegistry>().register(config)
    }
}
