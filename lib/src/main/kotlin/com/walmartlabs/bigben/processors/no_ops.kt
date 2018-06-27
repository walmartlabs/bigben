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
package com.walmartlabs.bigben.processors

import com.google.common.util.concurrent.Futures.immediateFuture
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.entities.Event
import com.walmartlabs.bigben.entities.EventResponse
import com.walmartlabs.bigben.utils.Json

/**
 * Created by smalik3 on 6/25/18
 */
class NoOpCustomClassProcessor(tenant: String, props: Json) : EventProcessor<Event> {
    override fun invoke(t: Event): ListenableFuture<Event> {
        return immediateFuture(t)
    }
}

class NoOpMessageProducerFactory : MessageProducerFactory {
    override fun create(tenant: String, props: Json): MessageProducer {
        return object : MessageProducer {
            override fun produce(e: EventResponse): ListenableFuture<*> {
                return immediateFuture(e)
            }
        }
    }
}
