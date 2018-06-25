package com.walmartlabs.bigben.core

import com.walmartlabs.bigben.entities.EventStatus
import com.walmartlabs.bigben.utils.logger
import java.time.ZonedDateTime
import java.util.*

internal data class BucketSnapshot(val id: ZonedDateTime, val count: Long, val processing: BitSet, val awaiting: BitSet) {

    companion object {
        private val l = logger<BucketSnapshot>()
        private val EMPTY = BitSet()

        fun with(id: ZonedDateTime, count: Long, shardSize: Int, status: EventStatus): BucketSnapshot {
            val shards = (if (count % shardSize == 0L) count / shardSize else count / shardSize + 1).toInt()
            val awaiting = if (count == 0L || EventStatus.PROCESSED == status) EMPTY else {
                BitSet(shards).apply { set(0, shards) }
            }
            when {
                count == 0L -> l.info("bucket: {} => empty, no events", id)
                awaiting === EMPTY -> l.info("bucket: {} => already done", id)
                else -> {
                    if (l.isInfoEnabled) l.info("bucket: {} => has {} events, resulting in {} shards", id, count, shards)
                }
            }
            return BucketSnapshot(id, count, BitSet(), awaiting)
        }
    }

    fun processing(shard: Int) = apply { awaiting.clear(shard); processing.set(shard) }

    fun done(shard: Int, status: EventStatus) {
        processing.clear(shard)
        when (status) {
            EventStatus.PROCESSED -> {
                if (l.isInfoEnabled) l.info("shard: {}[{}] finished successfully", id, shard)
                awaiting.clear(shard)
            }
            EventStatus.ERROR -> {
                if (l.isInfoEnabled) l.info("shard: {}[{}] finished with error", id, shard)
                awaiting.set(shard)
            }
            else -> throw IllegalArgumentException("invalid status value: $status")
        }
    }
}