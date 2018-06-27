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
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.walmartlabs.bigben.utils.Json
import com.walmartlabs.bigben.utils.logger
import java.util.*

/**
 * Created by smalik3 on 2/21/18
 */
object Props {

    private val l = logger<Props>()
    private val props: Json

    private val cache: Cache<String, Any> = CacheBuilder.newBuilder().build<String, Any>()

    init {
        val om = ObjectMapper(YAMLFactory())
        require(System.getProperty("props") != null) { "no 'props' system property found, system can't start" }
        l.info("initializing the properties")
        val p = System.getProperty("props")
        props = when {
            p.startsWith("file://") -> {
                val f = p.substring("file://".length)
                l.info("reading properties from the file: $f")
                val url = Props::class.java.classLoader.getResource(f)
                require(url != null) { "could not resolve $f to a url" }
                val x: Json = om.readValue(url, object : TypeReference<Json>() {}); x
            }
            p.startsWith("base64:") -> {
                l.info("decoding encoded properties")
                val x: Json = om.readValue(String(Base64.getDecoder().decode(p.substring("base64:".length))), object : TypeReference<Json>() {}); x
            }
            else -> throw IllegalArgumentException("unknown properties format: $p")
        }
    }

    fun exists(name: String): Boolean = get(name) != NULL

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

    private fun get(name: String, required: Boolean = false): Any? {
        val value = cache.get(name) { resolver(name) }
        if (value == NULL && required)
            throw IllegalArgumentException("no property with name: $name")
        else return value
    }

    private val NULL: Any = Any()

    private fun resolver(name: String, p: Json = props): Any {
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
