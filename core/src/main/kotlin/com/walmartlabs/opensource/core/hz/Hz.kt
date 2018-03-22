package com.walmartlabs.opensource.core.hz

import com.hazelcast.config.XmlConfigBuilder
import com.hazelcast.core.Hazelcast.newHazelcastInstance
import com.hazelcast.core.HazelcastInstance
import com.walmartlabs.opensource.core.logger
import com.walmartlabs.opensource.core.typeRefJson
import com.walmartlabs.opensource.core.utils.Props
import org.apache.commons.text.StrLookup
import org.apache.commons.text.StrSubstitutor
import java.io.ByteArrayInputStream

/**
 * Created by smalik3 on 2/23/18
 */

class Hz {

    companion object {
        private val l = logger<Hz>()
    }

    val hz: HazelcastInstance

    init {
        val config = typeRefJson<Map<String, Any>>(Props.string("bigben.hz.config", "{}")).let { map ->
            val template = String(Hz::class.java.getResourceAsStream(Props.string("hz.file", "/hz.template.xml")).run { readBytes().also { close() } })
            StrSubstitutor(object : StrLookup<Any>() {
                override fun lookup(key: String): String? {
                    return if (map.containsKey(key)) map[key]!!.toString()
                    else {
                        var current: Any? = map
                        for (it in key.split(".")) {
                            if (current is Map<*, *>)
                                current = current[it]
                            else break
                        }
                        current?.toString()
                    }
                }
            }).apply { setValueDelimiter(' ') }.replace(template)
        }
        hz = newHazelcastInstance(XmlConfigBuilder(ByteArrayInputStream(config.toByteArray())).build())
        if (l.isInfoEnabled) l.info("hazelcast config file: {}", config)
    }
}
