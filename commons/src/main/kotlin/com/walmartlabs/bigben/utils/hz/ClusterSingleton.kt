/*-
 * #%L
 * BigBen:commons
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
package com.walmartlabs.bigben.utils.hz

import com.google.common.collect.Sets.newConcurrentHashSet
import com.hazelcast.core.HazelcastInstanceNotActiveException
import com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.rootCause
import java.lang.Thread.currentThread
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newSingleThreadExecutor
import java.util.concurrent.atomic.AtomicInteger
import javax.print.attribute.standard.PrinterStateReason.SHUTDOWN

/**
 * Created by smalik3 on 3/1/18
 */
class ClusterSingleton(private val service: Service, private val hz: Hz) {

    private val listenerId = ConcurrentHashMap<String, String>()
    private val index = AtomicInteger()

    companion object {
        val ACTIVE_SERVICES: MutableSet<String> = newConcurrentHashSet<String>()!!
        private val l = logger<ClusterSingleton>()
        private val nonRetriables = setOf(HazelcastInstanceNotActiveException::class.java)
    }

    private val executor = newSingleThreadExecutor()

    init {
        executor.submit(task())
    }

    private fun task(): Runnable = Runnable {
        val lockName = "${service.name}_lock"
        try {
            currentThread().name = "${service.name}_service_thread"
            val clusterSingletonLock = hz.hz.getLock(lockName)
            clusterSingletonLock.lock()
            l.info(
                "cluster singleton elected, '${hz.hz.cluster.localMember.address}/${currentThread().name}' is the new owner for: ${service.name}"
            )
            listenerId.computeIfAbsent("listenerId") {
                hz.hz.apply { l.info("Adding the shutdown hook for cluster singleton: ${service.name}") }
                    .lifecycleService.addLifecycleListener { event ->
                    if (event.state == SHUTDOWN || event.state == SHUTTING_DOWN) {
                        if (l.isInfoEnabled) l.info("node is shutting down, destroying the service: {}", service.name)
                        try {
                            service.destroy()
                            ACTIVE_SERVICES.remove(service.name)
                        } catch (e: Exception) {
                            l.error("error in destroying the service: ${service.name}", e.rootCause())
                        }
                    }
                }.also {
                    l.info("initing the cluster singleton service: ${service.name}")
                    service.init()
                    ACTIVE_SERVICES.add(service.name)
                }
            }
            l.info("executing the cluster singleton service: ${service.name}")
            service.execute()
        } catch (e: Exception) {
            if (e.rootCause()!!::class.java in nonRetriables)
                l.error("error in running the service: ${service.name}", e.rootCause())
            else l.error("error in running the service: ${service.name}, retrying...", e.rootCause()) {
                try {
                    hz.hz.getLock(lockName).unlock()
                } catch (e: Exception) {
                    l.error("error in unlocking cluster singleton", e.rootCause())
                } finally {
                    l.info("resubmitting ownership claim task: attempt: ${index.incrementAndGet()}")
                    executor.submit(task())
                }
            }
        }
    }
}
