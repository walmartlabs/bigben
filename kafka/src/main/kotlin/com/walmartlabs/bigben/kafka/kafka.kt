package com.walmartlabs.bigben.kafka

import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.SettableFuture
import com.walmartlabs.bigben.BigBen.eventReceiver
import com.walmartlabs.bigben.entities.EventRequest
import com.walmartlabs.bigben.entities.EventResponse
import com.walmartlabs.bigben.processors.MessageProcessor
import com.walmartlabs.bigben.processors.MessageProducer
import com.walmartlabs.bigben.processors.MessageProducerFactory
import com.walmartlabs.bigben.utils.*
import com.walmartlabs.bigben.utils.utils.Props.long
import com.walmartlabs.bigben.utils.utils.Props.map
import com.walmartlabs.bigben.utils.utils.Props.string
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference


/**
 * Created by smalik3 on 6/25/18
 */
class KafkaMessageProducerFactory : MessageProducerFactory {
    override fun create(tenant: String, props: Map<String, Any>) = KafkaMessageProducer(tenant, props)
}

class KafkaMessageProducer(private val tenant: String, props: Map<String, Any>) : MessageProducer {

    companion object {
        val l = logger<KafkaMessageProducer>()
    }

    private val kafkaProducer = KafkaProducer<String, String>(props)
    private val topic = props["topic"]!!.toString()

    override fun produce(e: EventResponse): ListenableFuture<*> {
        if (l.isDebugEnabled) l.debug("tenant: $tenant, topic: $topic, event: ${e.eventId}")
        return SettableFuture.create<Any>().apply {
            kafkaProducer.send(ProducerRecord(topic, e.id, e.json())) { recordMetadata, exception ->
                if (exception != null) {
                    l.error("tenant: $tenant, topic: $topic, event: ${e.eventId}, failed", exception.rootCause())
                    setException(exception.rootCause()!!)
                } else if (l.isDebugEnabled) {
                    l.debug("tenant: $tenant, topic: $topic, event: ${e.eventId}, " +
                            "successful, partition: ${recordMetadata.partition()}, offset: ${recordMetadata.offset()}")
                    set(e)
                }
            }
        }
    }
}

class KafkaMessageProcessor : MessageProcessor {
    private val consumer = KafkaConsumer<String, String>(map("kafka.consumer.config"))
    private val topics = string("topics").split(",")
    private val closed = AtomicBoolean()

    companion object {
        private val l = logger<KafkaMessageProcessor>()
    }

    init {
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
                    val f = { records.map { eventReceiver.addEvent(EventRequest::class.java.fromJson(it.value())) }.reduce() }
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