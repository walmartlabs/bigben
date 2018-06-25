package com.walmartlabs.bigben.hz

import com.hazelcast.nio.serialization.DataSerializableFactory
import com.hazelcast.nio.serialization.IdentifiedDataSerializable
import com.walmartlabs.bigben.BigBen.entityProvider
import com.walmartlabs.bigben.api.EventReceiver.Companion.CACHED_PROCESSOR
import com.walmartlabs.bigben.entities.Bucket
import com.walmartlabs.bigben.entities.ShardStatus
import com.walmartlabs.bigben.entities.ShardStatusList
import com.walmartlabs.bigben.hz.HzObjectFactory.ObjectId.*
import com.walmartlabs.bigben.tasks.BulkShardTask
import com.walmartlabs.bigben.tasks.ShutdownTask
import com.walmartlabs.bigben.tasks.StatusTask

/**
 * Created by smalik3 on 3/10/18
 */
class HzObjectFactory : DataSerializableFactory {
    companion object {
        const val BIGBEN_FACTORY_ID = 1
    }

    enum class ObjectId {
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
            BUCKET -> entityProvider<Bucket>().let { it.raw(it.selector(Bucket::class.java)) }
        }
    }
}