package com.walmartlabs.opensource.core.hz

import com.google.common.collect.Sets.newConcurrentHashSet
import com.google.common.util.concurrent.Futures
import com.hazelcast.core.LifecycleEvent
import com.walmartlabs.opensource.core.logger
import com.walmartlabs.opensource.core.retriable
import com.walmartlabs.opensource.core.rootCause
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import javax.print.attribute.standard.PrinterStateReason

/**
 * Created by smalik3 on 3/1/18
 */
class ClusterSingleton(service: Service, hz: Hz) {

    private val listenerId = AtomicReference<String>()

    companion object {
        val ACTIVE_SERVICES = newConcurrentHashSet<String>()
        private val l = logger<ClusterSingleton>()
    }

    init {
        Executors.newSingleThreadExecutor().submit({
            try {
                Thread.currentThread().name = service.name + "_service_thread"
                val clusterSingletonLock = hz.hz.getLock(service.name + "_lock")
                clusterSingletonLock.lock()
                try {
                    if (l.isInfoEnabled) l.info("cluster singleton ownership claimed, '{}[{}]' is the new owner: {}",
                            hz.hz.cluster.localMember, Thread.currentThread().name, service.name)
                    if (listenerId.get() != null) {
                        if (l.isInfoEnabled) l.info("Adding the shutdown hook for cluster singleton: {}", service.name)
                        listenerId.set(hz.hz.lifecycleService.addLifecycleListener({ event ->
                            if (event.state == PrinterStateReason.SHUTDOWN || event.state == LifecycleEvent.LifecycleState.SHUTTING_DOWN) {
                                if (l.isInfoEnabled) l.info("node is shutting down, destroying the service: {}", service.name)
                                try {
                                    service.destroy()
                                    ACTIVE_SERVICES.remove(service.name)
                                } catch (e: Exception) {
                                    l.error("error in destroying the service: {}", service.name, e.rootCause())
                                }

                            }
                        }))
                    }
                    if (l.isInfoEnabled) l.info("starting the cluster singleton service: {}", service.name)
                    service.init()
                    ACTIVE_SERVICES.add(service.name);
                    {
                        if (l.isInfoEnabled) l.info("executing the cluster singleton service: {}", service.name)
                        service.execute()
                        Futures.immediateFuture<Any>(null)
                    }.retriable(service.name)

                } catch (e: Exception) {
                    l.error("Error in executing cluster singleton service, the service is un-deployed, " +
                            "and will not be re-deployed any more: {}", service.name, e.rootCause())
                    try {
                        service.destroy()
                    } catch (e1: Exception) {
                        l.error("error in destroying the service: {}", service.name, e1.rootCause())
                    }

                }
            } catch (e: Exception) {
                l.error("error in deploying the service: {}", service.name, e.rootCause())
            }
        })
    }

}