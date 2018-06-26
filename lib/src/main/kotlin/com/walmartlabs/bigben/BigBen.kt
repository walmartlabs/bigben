package com.walmartlabs.bigben

import com.walmartlabs.bigben.api.EventReceiver
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.core.ScheduleScanner
import com.walmartlabs.bigben.entities.EntityProvider
import com.walmartlabs.bigben.entities.EventLoader
import com.walmartlabs.bigben.processors.MessageProcessor
import com.walmartlabs.bigben.processors.MessageProducerFactory
import com.walmartlabs.bigben.processors.ProcessorRegistry
import com.walmartlabs.bigben.utils.hz.ClusterSingleton
import com.walmartlabs.bigben.utils.hz.Hz
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.utils.Props.exists
import com.walmartlabs.bigben.utils.utils.Props.string

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
    private val messageProcessor: MessageProcessor?

    @Suppress("UNCHECKED_CAST")
    inline fun <reified T> entityProvider() = entityProvider as EntityProvider<T>

    init {
        l.info("initializing entity provider")
        @Suppress("UNCHECKED_CAST")
        entityProvider = Class.forName(string("domain.entity.provider.class")).newInstance() as EntityProvider<Any>
        l.info("initializing event loader")
        eventLoader = if (entityProvider is EventLoader) entityProvider else Class.forName(string("domain.event.loader.class")).newInstance() as EventLoader
        l.info("loading processors")
        processorRegistry = ProcessorRegistry()
        l.info("initializing hazelcast")
        hz = Hz()
        l.info("initializing schedule scanner")
        val service = ScheduleScanner(hz)
        l.info("initializing cluster master")
        ClusterSingleton(service, hz)
        l.info("initializing event receiver")
        eventReceiver = EventReceiver(hz)
        l.info("initializing event service")
        eventService = EventService(hz, service, eventReceiver)
        l.info("initializing message producer factory")
        messageProducerFactory = Class.forName(string("messaging.producer.factory.class")).newInstance() as MessageProducerFactory
        messageProcessor = if (exists("messaging.processor.class")) {
            Class.forName(string("messaging.processor.class")).newInstance() as MessageProcessor
        } else null
        l.info("BigBen initialized successfully")
    }
}