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
package com.walmartlabs.bigben.utils.commons

import com.walmartlabs.bigben.utils.Json
import com.walmartlabs.bigben.utils.logger
import java.util.concurrent.ConcurrentHashMap

/**
 * Created by smalik3 on 9/17/18
 */
interface Module {
    fun init(registry: ModuleRegistry)
}

open class NoOpModule : Module {
    override fun init(registry: ModuleRegistry) {
    }
}

class ModuleRegistry {

    private val l = logger<ModuleRegistry>()

    val cache = ConcurrentHashMap<Class<*>, Any>()

    @Suppress("UNCHECKED_CAST")
    inline fun <reified T> module() = cache.computeIfAbsent(T::class.java) {
        cache.values.firstOrNull { T::class.java.isAssignableFrom(it::class.java) }
                ?: throw IllegalArgumentException("no module found with type: ${T::class.java}")
    } as T

    @Suppress("UNCHECKED_CAST")
    inline fun <reified T : Any> register(t: T) = cache.put(t::class.java, t as Any)

    fun loadModules(props: PropsLoader) {
        l.info("loading modules")
        props.list("modules").forEach {
            @Suppress("UNCHECKED_CAST")
            val p = Props.parse(it as Json)
            val enabled = p.boolean("enabled", true)
            if (!enabled) {
                l.info("skipping disabled module ${p.string("name")}")
            } else {
                l.info("initializing module: ${p.string("name")}")
                createModule(it).also {
                    it.init(this)
                    l.info("registering module: ${p.string("name")}")
                    register(it)
                }
            }
        }
    }

    private fun createModule(m: Json): Module {
        return try {
            (if (m.containsKey("class")) {
                (Class.forName(m["class"].toString()).newInstance() as Module)
            } else Class.forName(m["object"].toString()).getDeclaredField("INSTANCE").apply { isAccessible = true }.get(null) as Module)
        } catch (e: Exception) {
            e.printStackTrace()
            throw ExceptionInInitializerError(e)
        }
    }
}