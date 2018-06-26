package com.walmartlabs.bigben.utils.utils

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.walmartlabs.bigben.utils.logger
import java.util.*

/**
 * Created by smalik3 on 2/21/18
 */
object Props {

    private val l = logger<Props>()
    private val props: Map<String, Any>

    private val cache: Cache<String, Any> = CacheBuilder.newBuilder().build<String, Any>()

    init {
        System.setProperty("props", "file://bigben.yaml")
        val om = ObjectMapper(YAMLFactory())
        l.info("initializing the properties")
        props = if (System.getProperty("props") != null) {
            val p = System.getProperty("props")
            when {
                p.startsWith("file://") -> {
                    val f = p.substring("file://".length)
                    l.info("reading properties from the file: $f")
                    val url = Props::class.java.classLoader.getResource(f)
                    val x: Map<String, Any> = om.readValue(url, object : TypeReference<Map<String, Any>>() {}); x
                }
                p.startsWith("base64:") -> {
                    l.info("decoding encoded properties")
                    val x: Map<String, Any> = om.readValue(String(Base64.getDecoder().decode(p.substring("base64:".length))), object : TypeReference<Map<String, Any>>() {}); x
                }
                else -> throw IllegalArgumentException("unknown properties format: $p")
            }
        } else {
            l.warn("no properties supplied, did you set some?")
            emptyMap()
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
    fun map(name: String) = get(name, true) as Map<String, Any>

    private fun get(name: String, required: Boolean = false): Any? {
        val value = cache.get(name) { resolver(name) }
        if (value == NULL && required)
            throw IllegalArgumentException("no property with name: $name")
        else return value
    }

    private val NULL: Any = Any()

    private fun resolver(name: String, p: Map<String, Any> = props): Any {
        if (p.containsKey(name)) return p[name]!!
        else if (name.contains(".")) {
            val parts = name.split(".", limit = 2)
            return if (p.containsKey(parts[0]) && p[parts[0]] is Map<*, *>) {
                @Suppress("UNCHECKED_CAST")
                resolver(parts[1], p[parts[0]] as Map<String, Any>)
            } else NULL
        }
        return NULL
    }
}