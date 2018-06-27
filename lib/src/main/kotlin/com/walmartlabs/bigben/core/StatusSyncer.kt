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
package com.walmartlabs.bigben.core

import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.entities.Bucket
import com.walmartlabs.bigben.entities.Event
import com.walmartlabs.bigben.entities.EventStatus
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.extns.save
import com.walmartlabs.bigben.utils.done
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.retriable
import java.time.ZonedDateTime

internal class StatusSyncer {
    companion object {
        private val l = logger<StatusSyncer>()
    }

    fun syncBucket(bucketId: ZonedDateTime, status: EventStatus, setProcessedAt: Boolean, failedShards: Set<Int>?): ListenableFuture<Bucket> {
        if (l.isDebugEnabled) l.debug("bucket {} is done, syncing status as {}", bucketId, status)
        return {
            save<Bucket> {
                it.bucketId = bucketId; it.status = status
                if (setProcessedAt) it.processedAt = nowUTC()
                if (failedShards != null && failedShards.isNotEmpty()) it.failedShards = failedShards else it.failedShards = null
            }
        }.retriable().done({ l.error("bucket {} could not be synced with status {}, after multiple retries", bucketId, status, it) })
        { if (l.isInfoEnabled) l.info("bucket {} is successfully synced as {}", bucketId, status) }
    }

    fun syncShard(bucketId: ZonedDateTime, shard: Int, eventTime: ZonedDateTime, eventId: String, status: EventStatus, payload: String?): ListenableFuture<Event> {
        if (l.isDebugEnabled) l.debug("shard {}[{}] is done, syncing status as {}, payload: {}", bucketId, shard, status, payload)
        return save<Event> { it.id = eventId; it.eventTime = eventTime; it.status = status; if (payload != null) it.payload = payload }.done({ l.error("shard {}[{}] could not be synced with status {}, after multiple retries", bucketId, shard, status, it) }) {
            if (l.isInfoEnabled) l.info("shard {}[{}] is successfully synced with status {}", bucketId, shard, status)
        }
    }
}
