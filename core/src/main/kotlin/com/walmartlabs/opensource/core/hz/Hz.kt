package com.walmartlabs.opensource.core.hz

import com.hazelcast.config.XmlConfigBuilder
import com.hazelcast.core.Hazelcast.newHazelcastInstance
import com.hazelcast.core.HazelcastInstance
import com.walmartlabs.opensource.core.logger
import com.walmartlabs.opensource.core.typeRefJson
import com.walmartlabs.opensource.core.utils.Props
import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine
import org.apache.velocity.runtime.RuntimeConstants.RESOURCE_LOADER
import org.apache.velocity.runtime.log.NullLogChute
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader
import java.io.ByteArrayInputStream
import java.io.StringWriter

/**
 * Created by smalik3 on 2/23/18
 */

class Hz {

    companion object {
        private val VELOCITY_ENGINE = VelocityEngine()
        private val l = logger<Hz>()

        init {
            VELOCITY_ENGINE.setProperty(RESOURCE_LOADER, "classpath")
            VELOCITY_ENGINE.setProperty("classpath.resource.loader.class", ClasspathResourceLoader::class.java.name)
            VELOCITY_ENGINE.setProperty("runtime.log.logsystem.class", NullLogChute::class.java.name)
            VELOCITY_ENGINE.setProperty("runtime.log.logsystem.log4j.category", "velocity")
            VELOCITY_ENGINE.setProperty("runtime.log.logsystem.log4j.logger", "velocity")
            VELOCITY_ENGINE.init()
        }
    }

    val hz: HazelcastInstance

    init {
        val config = VelocityContext().let { c ->
            typeRefJson<Map<String, Any>>(Props.string("bigben.hz.config", "{}")).forEach { c.put(it.key, it.value) }
            val template = VELOCITY_ENGINE.getTemplate(Props.string("hz.file", "/hz.xml.vm"))
            StringWriter().apply { template.merge(c, this) }.toString()
        }
        hz = newHazelcastInstance(XmlConfigBuilder(ByteArrayInputStream(config.toByteArray())).build())
        if (l.isInfoEnabled) l.info("final hazelcast config file: {}", config)
    }
}
