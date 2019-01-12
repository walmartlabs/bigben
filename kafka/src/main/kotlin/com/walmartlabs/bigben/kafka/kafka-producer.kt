/*-
 * #%L
 * bigben-kafka
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
package com.walmartlabs.bigben.kafka

import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.SettableFuture
import com.walmartlabs.bigben.entities.EventResponse
import com.walmartlabs.bigben.processors.MessageProducer
import com.walmartlabs.bigben.processors.MessageProducerFactory
import com.walmartlabs.bigben.utils.Json
import com.walmartlabs.bigben.utils.commons.Module
import com.walmartlabs.bigben.utils.commons.ModuleRegistry
import com.walmartlabs.bigben.utils.json
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.rootCause
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * Created by smalik3 on 6/25/18
 */
class KafkaMessageProducerFactory : MessageProducerFactory, Module {
    override fun init(registry: ModuleRegistry) {
    }

    override fun create(tenant: String, props: Json) = KafkaMessageProducer(tenant, props)
}

open class KafkaMessageProducer(private val tenant: String, props: Json) : MessageProducer {

    companion object {
        val l = logger<KafkaMessageProducer>()
    }

    private val kafkaProducer = this.createProducer(props)
    private val topic = require(props.containsKey("topic")) { "no topic in props" }.run { props["topic"]!!.toString() }

    protected open fun createProducer(props: Json): Producer<String, String> =
        KafkaProducer<String, String>(props).apply { if (l.isInfoEnabled) l.info("kafka producer for tenant $tenant created successfully") }

    override fun produce(e: EventResponse): ListenableFuture<*> {
        if (l.isDebugEnabled) l.debug("producer:begin: tenant: $tenant, topic: $topic, event: ${e.id}")
        return SettableFuture.create<Any>().apply {
            kafkaProducer.send(ProducerRecord(topic, e.id, e.json())) { recordMetadata, exception ->
                if (exception != null) {
                    l.error("producer:error: tenant: $tenant, topic: $topic, event: ${e.id}, failure", exception.rootCause())
                    setException(exception.rootCause()!!)
                } else {
                    if (l.isDebugEnabled) l.debug(
                        "producer:success: tenant: $tenant, topic: $topic, event: ${e.id}, "
                                + "partition: ${recordMetadata.partition()}, offset: ${recordMetadata.offset()}"
                    )
                    set(e)
                }
            }
        }
    }
}