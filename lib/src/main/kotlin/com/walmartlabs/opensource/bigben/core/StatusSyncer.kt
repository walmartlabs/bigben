package com.walmartlabs.opensource.bigben.core

import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.opensource.bigben.entities.Bucket
import com.walmartlabs.opensource.bigben.entities.Event
import com.walmartlabs.opensource.bigben.entities.EventStatus
import com.walmartlabs.opensource.bigben.extns.nowUTC
import com.walmartlabs.opensource.bigben.extns.save
import com.walmartlabs.opensource.core.done
import com.walmartlabs.opensource.core.logger
import java.time.ZonedDateTime

internal class StatusSyncer {
    companion object {
        private val l = logger<StatusSyncer>()
    }

    fun syncBucket(bucketId: ZonedDateTime, status: EventStatus, setProcessedAt: Boolean): ListenableFuture<Bucket> {
        if (l.isDebugEnabled) l.debug("bucket {} is done, syncing status as {}", bucketId, status)
        return save<Bucket> { it.bucketId = bucketId; it.status = status; if (setProcessedAt) it.processedAt = nowUTC() }.
                done({ l.error("bucket {} could not be synced as {}, after multiple retries", bucketId, status, it) })
                { if (l.isInfoEnabled) l.info("bucket {} is successfully synced as {}", bucketId, status) }
    }

    fun syncShard(bucketId: ZonedDateTime, shard: Int, eventTime: ZonedDateTime, eventId: String, status: EventStatus, payload: String?): ListenableFuture<Event> {
        if (l.isDebugEnabled) l.debug("shard {}[{}] is done, syncing status as {}, payload: {}", bucketId, shard, status, payload)
        return save<Event> { it.id = eventId; it.eventTime = eventTime; it.status = status; if (payload != null) it.payload = payload }.
                done({ l.error("shard {}[{}] could not be synced with status {}, after multiple retries", bucketId, shard, status, it) }) {
                    if (l.isInfoEnabled) l.info("shard {}[{}] is successfully synced with status {}", bucketId, shard, status)
                }
    }
}