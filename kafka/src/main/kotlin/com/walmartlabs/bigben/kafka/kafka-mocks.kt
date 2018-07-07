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

import com.google.common.util.concurrent.Futures.immediateFailedFuture
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.entities.EventResponse
import com.walmartlabs.bigben.processors.MessageProducerFactory
import com.walmartlabs.bigben.utils.Json
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Created by smalik3 on 6/28/18
 */
class MockMessageProducerFactory : MessageProducerFactory {
    override fun create(tenant: String, props: Json) = object : KafkaMessageProducer(tenant, props) {
        override fun createProducer(props: Json) = MockProducer<String, String>(true, StringSerializer(), StringSerializer())
        override fun produce(e: EventResponse): ListenableFuture<*> {
            return if (props.containsKey("fail")) {
                immediateFailedFuture<Any>(Exception()) as ListenableFuture<*>
            } else super.produce(e)
        }
    }
}

class MockKafkaProcessor : KafkaMessageProcessor() {
    lateinit var consumer: MockConsumer<String, String>
    override fun createConsumer(): Consumer<String, String> = MockConsumer<String, String>(EARLIEST).apply { consumer = this }
}
