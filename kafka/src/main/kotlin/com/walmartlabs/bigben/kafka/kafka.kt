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
import com.walmartlabs.bigben.BigBen.eventReceiver
import com.walmartlabs.bigben.entities.EventRequest
import com.walmartlabs.bigben.entities.EventResponse
import com.walmartlabs.bigben.entities.Mode.UPSERT
import com.walmartlabs.bigben.processors.MessageProcessor
import com.walmartlabs.bigben.processors.MessageProducer
import com.walmartlabs.bigben.processors.MessageProducerFactory
import com.walmartlabs.bigben.utils.*
import com.walmartlabs.bigben.utils.commons.Props.long
import com.walmartlabs.bigben.utils.commons.Props.map
import com.walmartlabs.bigben.utils.commons.Props.string
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference


/**
 * Created by smalik3 on 6/25/18
 */
class KafkaMessageProducerFactory : MessageProducerFactory {
    override fun create(tenant: String, props: Json) = KafkaMessageProducer(tenant, props)
}

open class KafkaMessageProducer(private val tenant: String, props: Json) : MessageProducer {

    companion object {
        val l = logger<KafkaMessageProducer>()
    }

    private val kafkaProducer = this.createProducer(props)
    private val topic = require(props.containsKey("topic")) { "no topic in props" }.run { props["topic"]!!.toString() }

    protected open fun createProducer(props: Json): Producer<String, String> = KafkaProducer<String, String>(props)
            .apply { if (l.isInfoEnabled) l.info("kafka producer for tenant $tenant created successfully") }

    override fun produce(e: EventResponse): ListenableFuture<*> {
        if (l.isDebugEnabled) l.debug("tenant: $tenant, topic: $topic, event: ${e.id}")
        return SettableFuture.create<Any>().apply {
            kafkaProducer.send(ProducerRecord(topic, e.id, e.json())) { recordMetadata, exception ->
                if (exception != null) {
                    l.error("tenant: $tenant, topic: $topic, event: ${e.id}, failed", exception.rootCause())
                    setException(exception.rootCause()!!)
                } else if (l.isDebugEnabled) {
                    l.debug("producer:success: tenant: $tenant, topic: $topic, event: ${e.id}, " +
                            "partition: ${recordMetadata.partition()}, offset: ${recordMetadata.offset()}")
                    set(e)
                }
            }
        }
    }
}

open class KafkaMessageProcessor : MessageProcessor {
    private val consumer = this.createConsumer()
    private val topics = string("kafka.consumer.topics").split(",")
    private val closed = AtomicBoolean()

    companion object {
        private val l = logger<KafkaMessageProcessor>()
    }

    protected open fun createConsumer(): Consumer<String, String> = KafkaConsumer<String, String>(map("kafka.consumer.config"))

    override fun init() {
        if (l.isInfoEnabled) l.info("starting the kafka consumer for topic(s): $topics")
        consumer.subscribe(topics)
        val task = AtomicReference<ListenableFuture<List<EventResponse>>?>()
        val paused = mutableSetOf<TopicPartition>()
        while (!closed.get()) {
            if (l.isDebugEnabled) l.debug("starting the poll for topic(s): $topics")
            try {
                if (task.get() != null && task.get()!!.isDone) {
                    if (l.isDebugEnabled) l.debug("message processed for topic(s): $topics, resuming the partitions")
                    consumer.resume(paused)
                    paused.clear()
                    task.set(null)
                }
                if (l.isDebugEnabled) l.debug("polling the consumer for topic(s): $topics")
                val records = consumer.poll(long("kafka.consumer.poll.interval"))
                if (l.isDebugEnabled) l.debug("fetched ${records.count()} messages from topic(s): $topics")
                if (records.count() > 0) {
                    paused += topics.map {
                        if (l.isDebugEnabled) l.debug("pausing the partitions for topic: $it")
                        consumer.partitionsFor(it).map { TopicPartition(it.topic(), it.partition()) }.apply {
                            consumer.pause(this)
                            if (l.isDebugEnabled) l.debug("partitions paused for topic: $it")
                        }
                    }.flatten()
                    if (l.isDebugEnabled) l.debug("submitting records for processing for topic(s): $topics")
                    val f = {
                        records.map {
                            val eventRequest = EventRequest::class.java.fromJson(it.value())
                            if (eventRequest.mode == UPSERT) eventReceiver.addEvent(eventRequest)
                            else eventReceiver.removeEvent(eventRequest.id!!, eventRequest.tenant!!)
                        }.reduce()
                    }
                    task.set(f.retriable(maxRetries = 10))
                }
            } catch (e: Exception) {
                val rc = e.rootCause()
                if (rc is WakeupException) {
                    if (!closed.get())
                        l.warn("spurious consumer wakeup for topic(s): $topics", rc)
                    else l.info("consumer has been closed for topic(s): $topics")
                } else {
                    l.error("unknown exception, closing the consumer", rc)
                    closed.set(true)
                }
            }
        }
    }
}
