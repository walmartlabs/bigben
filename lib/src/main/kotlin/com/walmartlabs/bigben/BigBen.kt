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
package com.walmartlabs.bigben

import com.walmartlabs.bigben.api.EventReceiver
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.core.ScheduleScanner
import com.walmartlabs.bigben.entities.EntityProvider
import com.walmartlabs.bigben.entities.EventLoader
import com.walmartlabs.bigben.processors.MessageProcessor
import com.walmartlabs.bigben.processors.MessageProducerFactory
import com.walmartlabs.bigben.processors.ProcessorRegistry
import com.walmartlabs.bigben.utils.Json
import com.walmartlabs.bigben.utils.commons.Module
import com.walmartlabs.bigben.utils.commons.Props.boolean
import com.walmartlabs.bigben.utils.commons.Props.exists
import com.walmartlabs.bigben.utils.commons.Props.int
import com.walmartlabs.bigben.utils.commons.Props.list
import com.walmartlabs.bigben.utils.commons.Props.string
import com.walmartlabs.bigben.utils.hz.ClusterSingleton
import com.walmartlabs.bigben.utils.hz.Hz
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.rootCause
import java.util.Collections.unmodifiableMap
import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.exitProcess

/**
 * Created by smalik3 on 6/24/18
 */
object BigBen {
    private val l = logger<BigBen>()

    val eventService: EventService
    val eventReceiver: EventReceiver
    val entityProvider: EntityProvider<Any>
    val eventLoader: EventLoader
    val processorRegistry: ProcessorRegistry
    val hz: Hz
    val messageProducerFactory: MessageProducerFactory
    private val messageProcessors: List<MessageProcessor>
    lateinit var modules: Map<Class<*>, Module>

    @Suppress("UNCHECKED_CAST")
    inline fun <reified T> entityProvider() = entityProvider as EntityProvider<T>

    @Suppress("UNCHECKED_CAST")
    inline fun <reified T> module() = modules[T::class.java] as T

    init {
        try {
            if (System.getProperty("props") == null) {
                l.warn("missing 'props' system property, using the default one: bigben.yaml")
                System.setProperty("props", "file:///bigben.yaml")
            }
            l.info("initializing entity provider")
            @Suppress("UNCHECKED_CAST")
            entityProvider = Class.forName(string("domain.entity.provider.class")).newInstance() as EntityProvider<Any>
            l.info("initializing event loader")
            eventLoader = if (entityProvider is EventLoader) entityProvider else Class.forName(string("domain.event.loader.class")).newInstance() as EventLoader
            l.info("initializing message producer factory")
            messageProducerFactory = Class.forName(string("messaging.producer.factory.class")).newInstance() as MessageProducerFactory
            l.info("loading processors")
            processorRegistry = ProcessorRegistry()
            l.info("initializing hazelcast")
            hz = Hz()
            l.info("initializing schedule scanner")
            val service = ScheduleScanner(hz)
            if (boolean("events.disable.scheduler")) {
                l.info("skipping initializing cluster scheduler")
            } else {
                l.info("initializing cluster scheduler")
                ClusterSingleton(service, hz)
            }
            l.info("initializing event receiver")
            eventReceiver = EventReceiver(hz)
            l.info("initializing event service")
            eventService = EventService(hz, service, eventReceiver)
            messageProcessors = if (exists("messaging.processor.class")) {
                (1..int("kafka.num.consumers")).map { Class.forName(string("messaging.processor.class")).newInstance() as MessageProcessor }
            } else emptyList()
            if (messageProcessors.isNotEmpty()) {
                l.info("starting ${messageProcessors.size} message processors")
                val index = AtomicInteger(0)
                newFixedThreadPool(messageProcessors.size) { Thread(it, "messageProcessor#${index.getAndIncrement()}") }.apply {
                    messageProcessors.forEach { submit { it.init() } }
                }
            }
            l.info("BigBen initialized successfully")
        } catch (e: Throwable) {
            l.error("error in starting BigBen, the system will exit", e.rootCause())
            exitProcess(1)
        }
    }

    fun init() {
        modules = unmodifiableMap(if (exists("modules")) {
            @Suppress("UNCHECKED_CAST")
            val modules = list("modules") as List<Json>
            modules.map { m ->
                l.info("loading module: ${m["name"]}")
                (if (m.containsKey("class")) {
                    (Class.forName(m["class"].toString()).newInstance() as Module).apply { init() }.apply { l.info("module ${m["name"]} loaded successfully") }
                } else Class.forName(m["object"].toString()).getDeclaredField("INSTANCE").apply { isAccessible = true }.get(null) as Module).apply { init() }
            }.associate { it::class.java to it }
        } else emptyMap())
    }
}
