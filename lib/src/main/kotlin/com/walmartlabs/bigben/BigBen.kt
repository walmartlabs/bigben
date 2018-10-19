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

import com.walmartlabs.bigben.entities.EntityProvider
import com.walmartlabs.bigben.utils.commons.Module
import com.walmartlabs.bigben.utils.commons.ModuleRegistry
import com.walmartlabs.bigben.utils.commons.Props
import com.walmartlabs.bigben.utils.commons.Props.load
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.rootCause
import kotlin.system.exitProcess

/**
 * Created by smalik3 on 6/24/18
 */

object BigBen : Module {
    private val l = logger<BigBen>()

    val registry = ModuleRegistry()

    init {
        System.getProperty("bigben.props")?.run {
            l.info("using props from location: $this")
            load(System.getProperty("bigben.props"))
        } ?: {
            l.warn("missing 'bigben.props' system property, using the default: file://bigben.yaml")
            load("file://bigben.yaml")
        }()
        l.info("initiating module registration")
        try {
            registry.loadModules(Props)
        } catch (e: Exception) {
            l.error("error in loading modules, system will exit now", e.rootCause())
            //exitProcess(1)
            throw ExceptionInInitializerError(e.rootCause())
        }
        l.info("module registration is complete")
        l.info("BigBen initialized successfully")
    }

    inline fun <reified T> module() = registry.module<T>()
    inline fun <reified T> entityProvider() = registry.module<EntityProvider<T>>()

    override fun init(registry: ModuleRegistry) {

    }
}