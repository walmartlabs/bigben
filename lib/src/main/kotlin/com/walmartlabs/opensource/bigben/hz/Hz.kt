package com.walmartlabs.opensource.bigben.hz

import com.fasterxml.jackson.core.type.TypeReference
import com.hazelcast.config.XmlConfigBuilder
import com.hazelcast.core.Hazelcast.newHazelcastInstance
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.nio.serialization.DataSerializableFactory
import com.hazelcast.nio.serialization.IdentifiedDataSerializable
import com.walmartlabs.opensource.bigben.api.EventReceiver.Companion.CACHED_PROCESSOR
import com.walmartlabs.opensource.bigben.entities.ShardStatus
import com.walmartlabs.opensource.bigben.entities.ShardStatusList
import com.walmartlabs.opensource.bigben.extns.fromJson
import com.walmartlabs.opensource.bigben.extns.logger
import com.walmartlabs.opensource.bigben.hz.HzObjectFactory.OBJECT_ID.*
import com.walmartlabs.opensource.bigben.tasks.BulkShardTask
import com.walmartlabs.opensource.bigben.tasks.ShutdownTask
import com.walmartlabs.opensource.bigben.tasks.StatusTask
import com.walmartlabs.opensource.bigben.utils.Props
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
            val overrides = if (Props.string("hz.props", "{}") != "{}") Hz::class.java.getResource("hz.props").readText() else "{}"
            object : TypeReference<Map<String, Any>>() {}.fromJson(overrides).forEach { c.put(it.key, it.value) }
            val template = VELOCITY_ENGINE.getTemplate(Props.string("hz.file", "/hz.xml.vm"))
            StringWriter().apply { template.merge(c, this) }.toString()
        }
        hz = newHazelcastInstance(XmlConfigBuilder(ByteArrayInputStream(config.toByteArray())).build())
        if (l.isInfoEnabled) l.info("final hazelcast config file: {}", config)
    }
}

class HzObjectFactory : DataSerializableFactory {
    companion object {
        val BIGBEN_FACTORY_ID = 1
    }

    enum class OBJECT_ID {
        EVENT_RECEIVER_ADD_EVENT,
        BULK_EVENT_TASK,
        SHUTDOWN_TASK,
        CLUSTER_STATUS_TASK,
        SHARD_STATUS,
        SHARD_STATUS_LIST
    }

    override fun create(typeId: Int): IdentifiedDataSerializable {
        return when (values()[typeId]) {
            EVENT_RECEIVER_ADD_EVENT -> CACHED_PROCESSOR
            BULK_EVENT_TASK -> BulkShardTask()
            SHUTDOWN_TASK -> ShutdownTask()
            SHARD_STATUS -> ShardStatus()
            SHARD_STATUS_LIST -> ShardStatusList()
            CLUSTER_STATUS_TASK -> StatusTask()
        }
    }
}