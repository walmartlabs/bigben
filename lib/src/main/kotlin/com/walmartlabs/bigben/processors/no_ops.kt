package com.walmartlabs.bigben.processors

import com.google.common.util.concurrent.Futures.immediateFuture
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.entities.Event
import com.walmartlabs.bigben.entities.EventResponse

/**
 * Created by smalik3 on 6/25/18
 */
class NoOpCustomClassProcessor : EventProcessor<Event> {
    override fun invoke(t: Event): ListenableFuture<Event> {
        return immediateFuture(t)
    }
}

class NoOpMessageProducerFactory : MessageProducerFactory {
    override fun create(tenant: String, props: Map<String, Any>): MessageProducer {
        return object : MessageProducer {
            override fun produce(e: EventResponse): ListenableFuture<*> {
                return immediateFuture(e)
            }
        }
    }
}