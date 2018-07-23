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

import com.hazelcast.config.XmlConfigBuilder
import com.hazelcast.core.Hazelcast.newHazelcastInstance
import com.hazelcast.core.HazelcastInstance
import com.walmartlabs.bigben.utils.Json
import com.walmartlabs.bigben.utils.commons.Props.flattenedMap
import com.walmartlabs.bigben.utils.commons.Props.map
import com.walmartlabs.bigben.utils.commons.Props.string
import com.walmartlabs.bigben.utils.json
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.typeRefJson
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
        val config = typeRefJson<Json>(flattenedMap(map("hz")).json()).let { map ->
            if(l.isDebugEnabled) l.debug("using the hazelcast config from: ${Hz::class.java.getResource(string("hz.template"))}")
            val template = String(Hz::class.java.getResourceAsStream(string("hz.template")).run { readBytes().also { close() } })
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
        if (l.isDebugEnabled) l.debug("hazelcast config file: {}", config)
    }
}
