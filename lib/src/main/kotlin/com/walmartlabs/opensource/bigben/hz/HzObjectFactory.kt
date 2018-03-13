package com.walmartlabs.opensource.bigben.hz

import com.hazelcast.nio.serialization.DataSerializableFactory
import com.hazelcast.nio.serialization.IdentifiedDataSerializable
import com.walmartlabs.opensource.bigben.api.EventReceiver.Companion.CACHED_PROCESSOR
import com.walmartlabs.opensource.bigben.entities.Bucket
import com.walmartlabs.opensource.bigben.entities.ShardStatus
import com.walmartlabs.opensource.bigben.entities.ShardStatusList
import com.walmartlabs.opensource.bigben.extns.domainProvider
import com.walmartlabs.opensource.bigben.hz.HzObjectFactory.OBJECT_ID.*
import com.walmartlabs.opensource.bigben.tasks.BulkShardTask
import com.walmartlabs.opensource.bigben.tasks.ShutdownTask
import com.walmartlabs.opensource.bigben.tasks.StatusTask

/**
 * Created by smalik3 on 3/10/18
 */
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
        SHARD_STATUS_LIST,
        BUCKET
    }

    override fun create(typeId: Int): IdentifiedDataSerializable {
        return when (values()[typeId]) {
            EVENT_RECEIVER_ADD_EVENT -> CACHED_PROCESSOR
            BULK_EVENT_TASK -> BulkShardTask()
            SHUTDOWN_TASK -> ShutdownTask()
            SHARD_STATUS -> ShardStatus()
            SHARD_STATUS_LIST -> ShardStatusList()
            CLUSTER_STATUS_TASK -> StatusTask()
            BUCKET -> domainProvider<Bucket>().let { it.raw(it.selector(Bucket::class.java)) }
        }
    }
}