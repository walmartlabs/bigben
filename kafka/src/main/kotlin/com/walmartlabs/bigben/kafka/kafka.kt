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
import com.walmartlabs.bigben.utils.*
import com.walmartlabs.bigben.utils.commons.Module
import com.walmartlabs.bigben.utils.commons.ModuleLoader
import com.walmartlabs.bigben.utils.commons.Props
import com.walmartlabs.bigben.utils.commons.Props.boolean
import com.walmartlabs.bigben.utils.commons.Props.int
import com.walmartlabs.bigben.utils.commons.Props.long
import com.walmartlabs.bigben.utils.commons.Props.map
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference


/**
 * Created by smalik3 on 6/25/18
 */
class KafkaMessageProducerFactory : MessageProducerFactory, Module {
    override fun init(loader: ModuleLoader) {
    }

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
                    l.error("producer:error: tenant: $tenant, topic: $topic, event: ${e.id}, failure", exception.rootCause())
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

object KafkaModule : Module {

    private val l = logger<KafkaModule>()

    override fun init(loader: ModuleLoader) {
        l.info("initializing kafka processor(s)")
        Props.list("kafka").forEach {
            @Suppress("UNCHECKED_CAST")
            val p = Props.parse(it as Json)
            require(p.exists("config.group.id")) { "group.id is required" }
            val index = AtomicInteger(0)
            newFixedThreadPool(p.int("num.consumers")) { Thread(it, "kafkaProcessor[${p.string("config.group.id")}]#${index.getAndIncrement()}") }.apply {
                submit {
                    try {
                        "processor.class".apply {
                            require(p.exists(this)) { "$this is required" }
                            Class.forName(p.string(this)).newInstance()
                        }
                    } catch (e: Exception) {
                        l.error("unexpected error in starting kafka processor", e.rootCause())
                    }
                }
            }
        }
    }
}

abstract class KafkaMessageProcessor : Runnable {
    private val topics = Props.string("kafka.consumer.topics").split(",")
    private val closed = AtomicBoolean()
    private val autoCommit = boolean("kafka.consumer.config.enable.auto.commit")
    private var numUnknownExceptionRetries = int("kafka.unknown.exception.retries", 5)

    companion object {
        private val l = logger<KafkaMessageProcessor>()
    }

    abstract fun process(cr: ConsumerRecord<String, String>): ListenableFuture<Any>
    open fun createConsumer(): Consumer<String, String> = KafkaConsumer<String, String>(map("kafka.consumer.config"))

    override fun run() {
        val consumer = createConsumer()
        if (l.isInfoEnabled) {
            l.info("starting the kafka consumer for topic(s): $topics")
            if (!autoCommit)
                l.info("offsets will be committed manually")
        }
        val owned = AtomicReference<Set<TopicPartition>?>()
        consumer.subscribe(topics, object : ConsumerRebalanceListener {
            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
                if (l.isDebugEnabled) l.debug("partitions assigned: ${partitions.groupBy { it.topic() }.mapValues { it.value.map { it.partition() }.toSortedSet() }.toSortedMap()}")
                owned.set(partitions.toSet())
            }

            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
                if (l.isDebugEnabled) l.debug("partitions revoked: ${partitions.groupBy { it.topic() }.mapValues { it.value.map { it.partition() }.toSortedSet() }.toSortedMap()}")
                owned.set(null)
            }
        })
        val tasks = LinkedBlockingQueue<() -> Unit>()
        while (!closed.get()) {
            if (l.isDebugEnabled) l.debug("starting the poll for topic(s): $topics")
            try {
                mutableListOf<() -> Unit>().run { tasks.drainTo(this); this.forEach { it() } }
                if (l.isDebugEnabled) l.debug("polling the consumer for topic(s): $topics")
                val records = consumer.poll(long("kafka.consumer.poll.interval"))
                if (l.isDebugEnabled) l.debug("fetched ${records.count()} messages from topic(s): $topics")
                if (records.count() > 0) {
                    val (offsets, range) = records.groupBy { TopicPartition(it.topic(), it.partition()) }.run {
                        mapValues { OffsetAndMetadata(it.value.maxBy { it.offset() }!!.offset() + 1) } to
                                mapValues { "[${it.value.minBy { it.offset() }!!.offset()}-${it.value.maxBy { it.offset() }!!.offset()}]" }
                                        .mapKeys { "${it.key.topic()}[${it.key.partition()}]" }.toSortedMap()
                    }
                    val partitions = records.partitions().apply {
                        if (l.isDebugEnabled)
                            l.debug("pausing the partitions ${groupBy { it.topic() }.mapValues { it.value.map { it.partition() }.toSortedSet() }.toSortedMap()}")
                        consumer.pause(this)
                    }; {
                        if (l.isDebugEnabled)
                            l.debug("resuming the partitions ${partitions.groupBy { it.topic() }.mapValues { it.value.map { it.partition() }.toSortedSet() }.toSortedMap()}")
                        consumer.resume(partitions)
                        val ownedSnapshot = owned.get()
                        if (!autoCommit && ownedSnapshot != null) {
                            val filtered = offsets.filterKeys { it in ownedSnapshot }
                            if (l.isDebugEnabled) l.debug("committing offsets $filtered")
                            try {
                                consumer.commitSync(filtered)
                            } catch (e: CommitFailedException) {
                                l.warn("bulk commit failed for offsets: $filtered, trying to each owned partition commit one by one")
                                offsets.forEach {
                                    // no snapshot here
                                    if (owned.get() != null && it.key in owned.get()!!) {
                                        try {
                                            consumer.commitSync(mapOf(it.key to it.value))
                                        } catch (e: Exception) {
                                            l.warn("error in committing offset for ${it.key}, ignoring")
                                        }
                                    } else l.info("partition ${it.key} is no more owned by this consumer, ignoring the offset commit")
                                }
                            }
                        }
                    }.apply {
                        if (l.isDebugEnabled) l.debug("submitting records for processing: $range")
                        records.map { it ->
                            { process(it) }.retriable("${it.topic()}/${it.partition()}/${it.offset()}/${it.key()}", maxRetries = int("kafka.consumer.message.retry.max.count"))
                        }.reduce().transform { this }.done({
                            l.error("error in processing messages: $range", it.rootCause())
                            tasks.add(this); consumer.wakeup()
                        }) {
                            if (l.isDebugEnabled) l.debug("messages processed successfully: $range")
                            tasks.add(this); consumer.wakeup()
                        }
                    }
                }
            } catch (e: Exception) {
                val rc = e.rootCause()
                if (rc is WakeupException) {
                    if (!closed.get()) {
                        if (tasks.isNotEmpty())
                            l.warn("spurious consumer wakeup for topic(s): $topics", rc)
                        // else ignore the wakeup as it was intentional
                    } else l.info("consumer has been closed for topic(s): $topics")
                } else {
                    if (numUnknownExceptionRetries-- > 0) l.warn("unknown exception, ignoring", rc)
                    else l.error("unknown exception, giving up after $numUnknownExceptionRetries retries, closing the consumer", rc)
                    closed.set(true)
                }
            }
        }
    }
}
