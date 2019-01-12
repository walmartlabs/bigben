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
import com.google.common.util.concurrent.Futures.immediateFuture
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.BigBen
import com.walmartlabs.bigben.api.EventReceiver
import com.walmartlabs.bigben.entities.EventRequest
import com.walmartlabs.bigben.entities.EventStatus.*
import com.walmartlabs.bigben.entities.Mode.UPSERT
import com.walmartlabs.bigben.extns.event
import com.walmartlabs.bigben.processors.ProcessorRegistry
import com.walmartlabs.bigben.utils.commons.PropsLoader
import com.walmartlabs.bigben.utils.fromJson
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.rootCause
import com.walmartlabs.bigben.utils.transformAsync
import org.apache.kafka.clients.consumer.ConsumerRecord

class ProcessorImpl(props: PropsLoader) : KafkaMessageProcessor(props) {

    companion object {
        private val l = logger<ProcessorImpl>()
    }

    private val badMessageMarker = immediateFuture(null)
    private val eventReceiver = BigBen.module<EventReceiver>()
    private val processorRegistry = BigBen.module<ProcessorRegistry>()

    override fun process(cr: ConsumerRecord<String, String>): ListenableFuture<Any> {
        return ((try {
            EventRequest::class.java.fromJson(cr.value())
        } catch (e: Exception) {
            l.warn("bad message format, dropping: ${cr.value()}, error: ${e.rootCause()?.message}"); null
        })?.run {
            if (l.isDebugEnabled) l.debug("received audit event: $this")
            try {
                if (mode == UPSERT) eventReceiver.addEvent(this).transformAsync {
                    if (it!!.eventStatus == TRIGGERED) processorRegistry.invoke(it.event())
                    else if (it.eventStatus == ERROR || it.eventStatus == REJECTED) {
                        l.warn("event request is rejected or had error, event response: $it")
                    }
                    immediateFuture(it)
                } else eventReceiver.removeEvent(id!!, tenant!!)
            } catch (e: Exception) {
                val rc = e.rootCause()!!
                l.error("failed to process message: $cr", rc)
                immediateFailedFuture<Any>(rc)
            }
        } ?: badMessageMarker).run {
            @Suppress("UNCHECKED_CAST")
            this as ListenableFuture<Any>
        }
    }
}
