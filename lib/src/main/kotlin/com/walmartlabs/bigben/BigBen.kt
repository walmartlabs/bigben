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
import com.walmartlabs.bigben.entities.EntityProvider
import com.walmartlabs.bigben.entities.EventLoader
import com.walmartlabs.bigben.processors.ProcessorRegistry
import com.walmartlabs.bigben.utils.commons.Module
import com.walmartlabs.bigben.utils.commons.ModuleLoader
import com.walmartlabs.bigben.utils.commons.Props
import com.walmartlabs.bigben.utils.commons.Props.load
import com.walmartlabs.bigben.utils.hz.Hz
import com.walmartlabs.bigben.utils.logger

/**
 * Created by smalik3 on 6/24/18
 */

object BigBen : Module {
    private val l = logger<BigBen>()

    val loader = ModuleLoader()

    private val modules = loader.cache.apply {
        (if (System.getProperty("bigben.props") == null) {
            l.warn("missing 'bigben.props' system property, using the default: file://bigben.yaml")
            load("file://bigben.yaml")
        } else load(System.getProperty("bigben.props"))).let { loader.loadModules(Props) }
    }

    val eventService = loader.module<EventService>()
    val eventReceiver = loader.module<EventReceiver>()
    val entityProvider = loader.module<EntityProvider<Any>>()
    val eventLoader = loader.module<EventLoader>()
    val processorRegistry = loader.module<ProcessorRegistry>()
    val hz = loader.module<Hz>()

    @Suppress("UNCHECKED_CAST")
    inline fun <reified T> entityProvider() = loader.module<EntityProvider<T>>()

    override fun init(loader: ModuleLoader) {
        l.info("BigBen initialized successfully")
    }
}

object EventModule : Module {

    private val l = logger<EventModule>()

    override fun init(loader: ModuleLoader) {
        val hz = loader.module<Hz>()
        l.info("initializing event receiver")
        loader.register(EventReceiver(hz))
        l.info("initializing event service")
        loader.register(EventService(hz, loader.module(), loader.module()))
    }
}
