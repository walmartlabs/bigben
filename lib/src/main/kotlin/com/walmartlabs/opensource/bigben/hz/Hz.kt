package com.walmartlabs.opensource.bigben.hz

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.nio.serialization.DataSerializableFactory
import com.hazelcast.nio.serialization.IdentifiedDataSerializable
import com.walmartlabs.opensource.bigben.api.EventReceiver.Companion.CACHED_PROCESSOR
import com.walmartlabs.opensource.bigben.entities.ShardStatus
import com.walmartlabs.opensource.bigben.entities.ShardStatusList
import com.walmartlabs.opensource.bigben.hz.HzObjectFactory.OBJECT_ID.*
import com.walmartlabs.opensource.bigben.tasks.BulkShardTask
import com.walmartlabs.opensource.bigben.tasks.ShutdownTask
import com.walmartlabs.opensource.bigben.tasks.StatusTask

/**
 * Created by smalik3 on 2/23/18
 */

class Hz {
    val hz: HazelcastInstance
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