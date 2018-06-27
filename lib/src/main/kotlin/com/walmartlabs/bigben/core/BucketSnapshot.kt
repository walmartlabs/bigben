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

import com.walmartlabs.bigben.entities.EventStatus
import com.walmartlabs.bigben.entities.EventStatus.ERROR
import com.walmartlabs.bigben.entities.EventStatus.PROCESSED
import com.walmartlabs.bigben.utils.logger
import java.time.ZonedDateTime
import java.util.*

internal data class BucketSnapshot(val id: ZonedDateTime, val count: Long, val processing: BitSet, val awaiting: BitSet) {

    companion object {
        private val l = logger<BucketSnapshot>()
        private val EMPTY = BitSet()

        fun with(id: ZonedDateTime, count: Long, shardSize: Int, status: EventStatus): BucketSnapshot {
            val shards = (if (count % shardSize == 0L) count / shardSize else count / shardSize + 1).toInt()
            val awaiting = if (count == 0L || PROCESSED == status) EMPTY else {
                BitSet(shards).apply { set(0, shards) }
            }
            when {
                count == 0L -> if (l.isDebugEnabled) l.debug("bucket: {} => empty, no events", id)
                awaiting === EMPTY -> if (l.isDebugEnabled) l.debug("bucket: {} => already done", id)
                else -> {
                    if (l.isDebugEnabled) l.debug("bucket: {} => has {} events, resulting in {} shards", id, count, shards)
                }
            }
            return BucketSnapshot(id, count, BitSet(), awaiting)
        }
    }

    fun processing(shard: Int) = apply { awaiting.clear(shard); processing.set(shard) }

    fun done(shard: Int, status: EventStatus) {
        processing.clear(shard)
        when (status) {
            PROCESSED -> {
                if (l.isInfoEnabled) l.info("shard: {}[{}] finished successfully", id, shard)
                awaiting.clear(shard)
            }
            ERROR -> {
                if (l.isInfoEnabled) l.info("shard: {}[{}] finished with error", id, shard)
                awaiting.set(shard)
            }
            else -> throw IllegalArgumentException("invalid status value: $status")
        }
    }
}
