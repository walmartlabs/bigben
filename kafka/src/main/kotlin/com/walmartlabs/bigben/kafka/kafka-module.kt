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
import com.walmartlabs.bigben.utils.*
import com.walmartlabs.bigben.utils.commons.Module
import com.walmartlabs.bigben.utils.commons.ModuleRegistry
import com.walmartlabs.bigben.utils.commons.Props
import com.walmartlabs.bigben.utils.commons.PropsLoader
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import java.lang.Thread.currentThread
import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

object KafkaModule : Module {

    private val l = logger<KafkaModule>()

    override fun init(registry: ModuleRegistry) {
        l.info("initializing kafka processor(s)")
        Props.list("kafka.consumers").forEach {
            @Suppress("UNCHECKED_CAST")
            val p = Props.parse(it as Json)
            require(p.exists("config.group.id")) { "group.id is required" }
            val index = AtomicInteger(0)
            val numConsumers = p.int("num.consumers")
            l.info("creating $numConsumers kafka consumers")
            newFixedThreadPool(numConsumers) {
                Thread(it, "kafkaProcessor[${p.string("config.group.id")}]#${index.getAndIncrement()}")
            }.apply {
                (1..numConsumers).forEach {
                    l.debug("creating kafka consumer: $it")
                    submit(try {
                        "processor.impl.class".run {
                            require(p.exists(this)) { "$this is required" }
                            Class.forName(p.string(this)).let {
                                require(KafkaMessageProcessor::class.java.isAssignableFrom(it)) { "processor class must extend ${KafkaMessageProcessor::class.java.simpleName}" }
                                it.getConstructor(PropsLoader::class.java).newInstance(p) as Runnable
                            }
                        }
                    } catch (e: Exception) {
                        l.error("unexpected error in starting kafka processor", e.rootCause())
                        throw IllegalArgumentException(e)
                    })
                }
            }
        }
    }
}

abstract class KafkaMessageProcessor(private val props: PropsLoader) : Runnable {
    private val topics = props.string("topics").split(",")
    private val closed = AtomicBoolean()
    private val autoCommit = props.boolean("config.enable.auto.commit")
    private var numUnknownExceptionRetries = props.int("unknown.exception.retries", 5)

    companion object {
        private val l = logger<KafkaMessageProcessor>()
    }

    abstract fun process(cr: ConsumerRecord<String, String>): ListenableFuture<Any>
    open fun createConsumer(): Consumer<String, String> = KafkaConsumer<String, String>(props.map("config"))

    override fun run() {
        try {
            process()
        } catch (e: Exception) {
            l.error("error in running kafka processor", e.rootCause())
        }
    }

    private fun process() {
        val consumer = createConsumer()
        if (l.isInfoEnabled) {
            l.info("starting the kafka consumer ${currentThread().name} for topic(s): $topics")
            if (!autoCommit)
                l.info("offsets will be committed manually")
        }
        val owned = AtomicReference<Set<TopicPartition>?>()
        consumer.subscribe(topics, object : ConsumerRebalanceListener {
            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
                if (l.isDebugEnabled) l.debug("partitions assigned: ${partitions.groupBy { it.topic() }.mapValues {
                    it.value.map { it.partition() }.toSortedSet()
                }.toSortedMap()}")
                owned.set(partitions.toSet())
            }

            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
                if (l.isDebugEnabled) l.debug("partitions revoked: ${partitions.groupBy { it.topic() }.mapValues {
                    it.value.map { it.partition() }.toSortedSet()
                }.toSortedMap()}")
                owned.set(null)
            }
        })
        val tasks = LinkedBlockingQueue<() -> Unit>()
        val inPoll = AtomicBoolean(false)
        while (!closed.get()) {
            try {
                mutableListOf<() -> Unit>().run { tasks.drainTo(this); this.forEach { it() } }
                inPoll.set(true)
                if (l.isDebugEnabled) l.debug("starting the poll for topic(s): $topics")
                val records = consumer.poll(props.long("max.poll.wait.time"))
                inPoll.set(false)
                if (l.isDebugEnabled) l.debug("fetched ${records.count()} messages from topic(s): $topics")
                if (records.count() > 0) {
                    val (offsets, range) = records.groupBy { TopicPartition(it.topic(), it.partition()) }.run {
                        mapValues { OffsetAndMetadata(it.value.maxBy { it.offset() }!!.offset() + 1) } to
                                mapValues { "[${it.value.minBy { it.offset() }!!.offset()}-${it.value.maxBy { it.offset() }!!.offset()}]" }
                                    .mapKeys { "${it.key.topic()}[${it.key.partition()}]" }.toSortedMap()
                    }
                    val partitions = records.partitions().apply {
                        if (l.isDebugEnabled)
                            l.debug("pausing the partitions ${groupBy { it.topic() }.mapValues {
                                it.value.map { it.partition() }.toSortedSet()
                            }.toSortedMap()}")
                        consumer.pause(this)
                    }; {
                        if (l.isDebugEnabled)
                            l.debug("resuming the partitions ${partitions.groupBy { it.topic() }.mapValues {
                                it.value.map { it.partition() }.toSortedSet()
                            }.toSortedMap()}")
                        consumer.resume(partitions intersect (owned.get() ?: emptySet()))
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
                            { process(it) }.retriable(
                                "${it.topic()}/${it.partition()}/${it.offset()}/${it.key()}",
                                maxRetries = props.int("message.retry.max.count")
                            )
                        }.reduce().transform { this }.done({
                            l.error("error in processing messages: $range", it.rootCause())
                            tasks.add(this)
                            if (l.isDebugEnabled) l.debug("adding tasks for partition resume and offset commits")
                            if (inPoll.get()) {
                                if (l.isDebugEnabled) l.debug("waking up consumer stuck in poll")
                                consumer.wakeup()
                            }
                        }) {
                            if (l.isDebugEnabled) {
                                l.debug("messages processed successfully: $range")
                                l.debug("adding tasks for partition resume and offset commits")
                            }
                            tasks.add(this)
                            if (inPoll.get()) {
                                if (l.isDebugEnabled) l.debug("waking up consumer stuck in poll")
                                consumer.wakeup()
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                val rc = e.rootCause()
                if (rc is WakeupException) {
                    if (closed.get()) {
                        l.info("consumer has been closed for topic(s): $topics")
                    }
                } else {
                    if (numUnknownExceptionRetries-- > 0) l.warn("unknown exception, ignoring", rc)
                    else l.error(
                        "unknown exception, giving up after $numUnknownExceptionRetries retries, closing the consumer",
                        rc
                    )
                    closed.set(true)
                }
            }
        }
    }
}