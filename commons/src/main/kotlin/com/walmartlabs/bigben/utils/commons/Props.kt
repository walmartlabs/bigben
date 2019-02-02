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
import com.walmartlabs.bigben.utils.Json
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.omYaml
import com.walmartlabs.bigben.utils.yaml
import org.apache.commons.text.StrLookup
import org.apache.commons.text.StrSubstitutor
import java.io.File
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
        private val l = logger<PropsLoader>()

        private val NULL: Any = Any()
        private val NULL_PRESENT: Any = Any()

        @Suppress("UNCHECKED_CAST")
        fun flatten(json: Json): Json {
            return json.entries.map { e ->
                if (e.value !is Map<*, *>) listOf(e.key to e.value) else {
                    flatten(e.value as Json).entries.map { "${e.key}.${it.key}" to it.value }
                }
            }.flatten().associate { it.first to it.second }
        }

        private fun unflatten(json: Json): Json {
            val result = mutableMapOf<String, Any>()
            val multiple =
                json.entries.filter { it.key.contains(".") }.map { it.key.split(".")[0] to 1 }.groupBy { it.first }
                    .mapValues { it.value.size }.filter { it.value > 1 }
            json.filter { it.key.split(".")[0] !in multiple.keys }.run { result.putAll(this) }
            multiple.forEach { e ->
                result[e.key.split(".")[0]] =
                        unflatten(json.filterKeys { it.startsWith(e.key + ".") }.mapKeys { it.key.split(".", limit = 2)[1] })
            }
            return result
        }

        fun merge(base: Json, override: Json, onlyUpdates: Boolean = false): Json {
            val fBase = flatten(base)
            val fOverrides = flatten(override)
            return merge0(fBase, fOverrides, onlyUpdates)
        }

        private fun merge0(base: Json, override: Json, onlyUpdates: Boolean = false): Json {
            val result = mutableMapOf<String, Any>().apply { putAll(base) }
            val added = override.keys - base.keys
            val updates = base.keys.intersect(override.keys)
            try {
                if (!onlyUpdates) added.forEach { if (override[it] != null) require(result.putIfAbsent(it, override[it]!!) == null) }
            } catch (e: Exception) {
                println(e)
            }
            updates.forEach {
                val baseVal = base[it]
                val overrideVal = override[it]!!
                when (baseVal) {
                    is Map<*, *> -> {
                        require(overrideVal is Map<*, *>) { "incompatible values for key $it: $baseVal, $overrideVal" }
                        @Suppress("UNCHECKED_CAST")
                        result[it] = merge(baseVal as Json, overrideVal as Json)
                    }
                    is Collection<*> -> {
                        require(overrideVal is Collection<*>) { "incompatible values for key $it: $baseVal, $overrideVal" }
                        result[it] = LinkedHashSet(baseVal) + LinkedHashSet(overrideVal)
                    }
                    else -> {
                        result[it] = overrideVal
                    }
                }
            }
            return result
        }

        private fun substitute(json: Json): Json {
            val s1 = StrSubstitutor(StrLookup.systemPropertiesLookup())
            val s2 = StrSubstitutor(object : StrLookup<Any>() {
                override fun lookup(key: String): String? {
                    return System.getenv(key)
                }
            })
            val s3 = StrSubstitutor(StrLookup.mapLookup(json))
            return substitute0(substitute0(substitute0(json, s1), s2), s3) as Json
        }

        private fun substitute0(obj: Any?, substitutor: StrSubstitutor): Any? {
            return when (obj) {
                null -> null
                is Map<*, *> -> {
                    obj.entries.associate { substitute0(it.key, substitutor) to substitute0(it.value, substitutor) }
                }
                is Collection<*> -> {
                    obj.map { substitute0(it, substitutor) }
                }
                is String -> {
                    substitutor.replace(obj)
                }
                else -> obj
            }
        }
    }

    private val props = AtomicReference<Json>().apply { preloaded?.let { set(it) } }
    private val cache = CacheBuilder.newBuilder().build<String, Any>()

    fun load(supplier: Supplier<String>) = load(supplier.get())

    fun load(vararg props: String): PropsLoader {
        l.info("loading props")
        props.reversed().map {
            val x: Json = omYaml.readValue(ResourceLoader.load(it), object : TypeReference<Json>() {}); x to false
        }.run { this + (System.getenv() to true) + (System.getProperties() as Json to true) }
            .fold(emptyMap<String, Any>()) { r, e -> merge(r, e.first, e.second) }
            .run { substitute(this) }.run { unflatten(this) }.let { this.props.set(it) }
        cache.invalidateAll()
        l.info("loaded props successfully")
        if (l.isDebugEnabled) l.debug("resolved props:\n ${this.props.get().yaml()}")
        return this
    }

    fun exists(name: String): Boolean = get(name) != null
    fun int(name: String, defaultValue: Int = 0) = get(name)?.toString()?.toInt() ?: defaultValue
    fun long(name: String, defaultValue: Long = 0) = get(name)?.toString()?.toLong() ?: defaultValue
    fun string(name: String, defaultValue: String = "") = get(name)?.toString() ?: defaultValue
    fun boolean(name: String, defaultValue: Boolean = false) = get(name)?.toString()?.toBoolean() ?: defaultValue

    fun int(name: String) = get(name, true)!!.toString().toInt()
    fun long(name: String) = get(name, true)!!.toString().toLong()
    fun string(name: String) = get(name, true)!!.toString()
    fun boolean(name: String) = get(name, true)!!.toString().toBoolean()
    @Suppress("UNCHECKED_CAST")
    fun map(name: String) = get(name, true) as Json
    fun root() = props.get()!!

    @Suppress("UNCHECKED_CAST")
    fun list(name: String) = get(name, true) as List<Any>

    private fun get(name: String, required: Boolean = false): Any? {
        val value = cache.get(name) { resolver(name) }
        return when {
            value == NULL && required -> throw IllegalArgumentException("no property with name: $name")
            value == NULL -> null
            value == NULL_PRESENT && required -> throw IllegalArgumentException("property '$name' has a 'null' value")
            else -> value
        }
    }

    private fun resolver(name: String, p: Json = props.get()): Any {
        if (p.containsKey(name)) return p[name]?.let { it } ?: NULL_PRESENT
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

object ResourceLoader {

    private val l = logger<ResourceLoader>()

    fun load(location: String): String {
        return when {
            location.startsWith("uri://") -> {
                val f = File(location.substring("uri://".length))
                l.info("reading data from the resource: $f")
                require(f.exists()) { "could not resolve $f to a location" }
                f.readText()
            }
            location.startsWith("file://") -> {
                val f = location.substring("file://".length)
                l.info("reading data from the resource: $f")
                val ins = Props::class.java.classLoader.getResourceAsStream(f)
                require(ins != null) { "could not resolve $f to a location" }
                String(ins.readBytes())
            }
            else -> throw IllegalArgumentException("unknown resource format: $location")
        }
    }
}