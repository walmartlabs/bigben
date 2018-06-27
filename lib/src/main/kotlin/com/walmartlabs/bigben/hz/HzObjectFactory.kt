/*-
 * #%L
 * BigBen:lib
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
