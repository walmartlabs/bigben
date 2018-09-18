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

import com.fasterxml.jackson.core.type.TypeReference
import com.google.common.cache.CacheBuilder
import com.walmartlabs.bigben.utils.*
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

/**
 * Created by smalik3 on 2/21/18
 */
object Props : PropsLoader() {
    fun parse(props: Json) = PropsLoader(props)
}

open class PropsLoader(preloaded: Json? = null) {

    companion object {
        private val NULL: Any = Any()
    }

    private val l = logger<PropsLoader>()
    private val props = AtomicReference<Json>().apply { preloaded?.let { set(it) } }
    private val cache = CacheBuilder.newBuilder().build<String, Any>()

    fun load(supplier: Supplier<String>) = load(supplier.get())

    fun load(props: String) {
        l.info("loading props")
        this.props.set(when {
            props.startsWith("file://") -> {
                val f = props.substring("file://".length)
                l.info("reading properties from the file: $f")
                val url = Props::class.java.classLoader.getResource(f)
                require(url != null) { "could not resolve $f to a url" }
                val x: Json = omYaml.readValue(url, object : TypeReference<Json>() {}); x
            }
            props.startsWith("base64://") -> {
                l.info("loading properties from encoded string")
                val x: Json = omYaml.readValue(String(Base64.getDecoder().decode(props.substring("base64://".length))), object : TypeReference<Json>() {}); x
            }
            props.startsWith("yaml://") -> {
                l.info("loading properties from yaml configuration")
                val x: Json = omYaml.readValue(props.substring("yaml://".length), object : TypeReference<Json>() {}); x
            }
            props.startsWith("json://") -> {
                l.info("loading properties from json configuration")
                val x: Json = om.readValue(props.substring("json://".length), object : TypeReference<Json>() {}); x
            }
            else -> throw IllegalArgumentException("unknown properties format: $props")
        }).apply { cache.invalidateAll() }
        if (l.isDebugEnabled) l.debug("loaded props: \n${this.props.get().yaml()}")
    }

    fun exists(name: String): Boolean = get(name) != null

    fun int(name: String, defaultValue: Int = 0) = get(name) as? Int ?: defaultValue

    fun long(name: String, defaultValue: Long = 0) = get(name)?.toString()?.toLong()
            ?: defaultValue

    fun string(name: String, defaultValue: String = "") = get(name)?.toString()
            ?: defaultValue

    fun boolean(name: String, defaultValue: Boolean = false) = get(name)?.toString()?.toBoolean()
            ?: defaultValue

    fun int(name: String) = get(name, true)!! as Int
    fun long(name: String) = get(name, true)!!.toString().toLong()
    fun string(name: String) = get(name, true)!!.toString()
    fun boolean(name: String) = get(name, true)!!.toString().toBoolean()
    @Suppress("UNCHECKED_CAST")
    fun map(name: String) = get(name, true) as Json

    @Suppress("UNCHECKED_CAST")
    fun list(name: String) = get(name, true) as List<Any>

    @Suppress("UNCHECKED_CAST")
    fun flattenedMap(json: Json): Json {
        return json.entries.map { e ->
            if (e.value !is Map<*, *>) listOf(e.key to e.value) else {
                flattenedMap(e.value as Json).entries.map { "${e.key}.${it.key}" to it.value }
            }
        }.flatten().associate { it.first to it.second }
    }

    private fun get(name: String, required: Boolean = false): Any? {
        val value = cache.get(name) { resolver(name) }
        return when {
            value == NULL && required -> throw IllegalArgumentException("no property with name: $name")
            value == NULL -> null
            else -> value
        }
    }

    private fun resolver(name: String, p: Json = props.get()): Any {
        if (p.containsKey(name)) return p[name]!!
        else if (name.contains(".")) {
            val parts = name.split(".", limit = 2)
            return if (p.containsKey(parts[0]) && p[parts[0]] is Map<*, *>) {
                @Suppress("UNCHECKED_CAST")
                resolver(parts[1], p[parts[0]] as Json)
            } else NULL
        }
        return NULL
    }
}