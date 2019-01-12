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
package com.walmartlabs.bigben

import com.walmartlabs.bigben.api.EventReceiver
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.core.ScheduleScanner
import com.walmartlabs.bigben.utils.commons.Module
import com.walmartlabs.bigben.utils.commons.ModuleRegistry
import com.walmartlabs.bigben.utils.commons.Props
import com.walmartlabs.bigben.utils.hz.ClusterSingleton
import com.walmartlabs.bigben.utils.hz.Hz
import com.walmartlabs.bigben.utils.logger

/**
 * Created by smalik3 on 9/18/18
 */
object EventModule : Module {

    private val l = logger<EventModule>()

    override fun init(registry: ModuleRegistry) {
        val hz = registry.module<Hz>()
        l.info("initializing event receiver")
        registry.register(EventReceiver(hz))
        l.info("initializing event service")
        registry.register(EventService(hz, registry.module(), registry.module()))
    }
}

object SchedulerModule : Module {

    private val l = logger<SchedulerModule>()

    override fun init(registry: ModuleRegistry) {
        val scheduler = ScheduleScanner(registry.module())
        registry.register(scheduler)
        if (Props.boolean("events.scheduler.enabled")) {
            l.info("initializing cluster singleton")
            ClusterSingleton(scheduler, registry.module())
        } else {
            l.info("skipping initializing cluster scheduler")
        }
        l.info("Scheduler module initialized successfully")
    }
}
