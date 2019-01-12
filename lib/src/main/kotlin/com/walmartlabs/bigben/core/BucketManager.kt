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

import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimap
import com.google.common.util.concurrent.Futures.immediateFuture
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListenableScheduledFuture
import com.google.common.util.concurrent.MoreExecutors.listeningDecorator
import com.walmartlabs.bigben.BigBen.entityProvider
import com.walmartlabs.bigben.core.ScheduleScanner.Companion.BUCKET_CACHE
import com.walmartlabs.bigben.entities.Bucket
import com.walmartlabs.bigben.entities.EventStatus
import com.walmartlabs.bigben.entities.EventStatus.*
import com.walmartlabs.bigben.extns.toSet
import com.walmartlabs.bigben.utils.*
import com.walmartlabs.bigben.utils.commons.Props.int
import com.walmartlabs.bigben.utils.hz.Hz
import java.time.ZonedDateTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by smalik3 on 2/21/18
 */
class BucketManager(private val maxBuckets: Int, private val maxProcessingTime: Int, private val bucketWidth: Int, hz: Hz) {

    companion object {
        private val l = logger<BucketManager>()
        private val index = AtomicInteger()
        val scheduler = listeningDecorator(ScheduledThreadPoolExecutor(4, ThreadFactory { Thread(it, "BucketManager-${index.getAndIncrement()}") }))!!

        internal fun emptyBucket(bucketId: ZonedDateTime) = entityProvider<Bucket>().let { it.raw(it.selector(Bucket::class.java)).apply { this.bucketId = bucketId; count = 0L; status = EMPTY } }
    }

    private val shardSize = int("events.receiver.shard.size")
    private val statusSyncer = StatusSyncer()
    private val buckets = ConcurrentHashMap<ZonedDateTime, BucketSnapshot>()
    private val cache = hz.hz.getMap<ZonedDateTime, Bucket>(BUCKET_CACHE)

    @Volatile
    private var bucketsLoader: BucketsLoader? = null

    fun getProcessableShardsForOrBefore(bucketId: ZonedDateTime): ListenableFuture<out Multimap<ZonedDateTime, Int>> {
        if (bucketsLoader == null) {
            if (l.isInfoEnabled) l.info("starting the background load of previous buckets")
            val fetchSize = int("buckets.background.load.fetch.size")
            bucketsLoader = BucketsLoader(maxBuckets - 1, fetchSize, bucketWidth, bucketId) {
                buckets[it.bucketId!!] = when (it.status) {
                    in setOf(null, EMPTY, UN_PROCESSED, PROCESSED) -> BucketSnapshot.with(it.bucketId!!, it.count!!, shardSize, it.status ?: UN_PROCESSED)
                    ERROR -> {
                        require(it.failedShards != null && it.failedShards!!.isNotEmpty()) { "${it.bucketId} is marked $ERROR but has no failed shards information" }
                        if (l.isInfoEnabled) l.info("bucket ${it.bucketId} has shard failures: ${it.failedShards}, scheduling them for reprocessing")
                        BucketSnapshot(it.bucketId!!, it.count!!, BitSet(), it.failedShards!!.fold(BitSet()) { b, i -> b.apply { set(i) } })
                    }
                    else -> throw IllegalArgumentException("invalid bucket status: $it")
                }
            }.apply { run() }
        }
        return HashMultimap.create<ZonedDateTime, Int>().let { shards ->
            cache.getAsync(bucketId).listenable().transform {
                val bucket = it ?: emptyBucket(bucketId)
                if (buckets.putIfAbsent(bucketId, BucketSnapshot.with(bucketId, bucket.count!!, shardSize, bucket.status!!)) != null) {
                    l.warn("bucket with bucketId {} already existed in the cache, this is highly unusual", bucketId)
                }
                buckets.entries.filter { e -> e.value.awaiting.cardinality() > 0 }.forEach { e -> e.value.awaiting.stream().forEach { s -> shards.put(e.key, s) } }
                if (l.isInfoEnabled) l.info("processable shards at bucket: {}, are => {}", bucketId, shards)
                if (!shards.containsKey(bucketId)) {
                    if (l.isInfoEnabled) l.info("no events in the bucket: {}", bucketId)
                }; shards
            }.catching { e -> shards.also { l.warn("error in loading bucket: {}, will be retried again during next scan", bucketId, e.rootCause()) } }
        }
    }

    internal fun registerForProcessing(pairs: Collection<Pair<ZonedDateTime, Int>>) {
        pairs.forEach { p -> buckets[p.first]!!.processing(p.second) }
        purgeIfNeeded()
        startShardsTimer(pairs)
    }

    private fun startShardsTimer(pairs: Collection<Pair<ZonedDateTime, Int>>): ListenableScheduledFuture<*> {
        return pairs.sortedWith(Comparator { p1, p2 ->
            p1.first.compareTo(p2.first).let { if (it != 0) it else p1.second.compareTo(p2.second) }
        }).map { "${it.first}[${it.second}]" }.toList().let {
            if (l.isDebugEnabled) l.debug("starting processing timer for shards: {}", it)
            scheduler.schedule({ checkShardsStatus(pairs, it) }, maxProcessingTime.toLong(), SECONDS)
        }
    }

    @Synchronized
    private fun checkShardsStatus(pairs: Collection<Pair<ZonedDateTime, Int>>, shards: List<String>) {
        try {
            pairs.forEach {
                val bd = buckets[it.first]
                if (bd != null && bd.processing.get(it.second)) {
                    l.warn("bulk timer for shard: {}[{}] expired, marking the shard as failure", it.first, it.second)
                    bd.done(it.second, ERROR)
                }
            }
        } catch (e: Exception) {
            l.error("error in timing out the shards for processing, shards: {}", shards, e)
        }
    }

    @Synchronized
    internal fun shardDone(bucketId: ZonedDateTime, shard: Int?, status: EventStatus) {
        val bd = buckets[bucketId]
        if (bd == null) {
            l.warn("bucket {} not found in cache, might have been purged, ignoring this call", bucketId)
            return
        }
        bd.done(shard!!, status)
    }

    private val noOp = immediateFuture<Bucket>(null)

    @Synchronized
    internal fun bucketProcessed(bucketId: ZonedDateTime, status: EventStatus): ListenableFuture<Bucket> {
        val bd = buckets[bucketId]
        if (bd == null) {
            l.warn("bucket {} not found in cache, this is extremely unusual", bucketId)
            return noOp
        }
        bd.processing.clear()
        if (status == PROCESSED) {
            if (l.isInfoEnabled) l.info("bucket {} done, marking it as {}, all shards done", bucketId, status)
            bd.awaiting.clear()
        } else if (status == ERROR)
            l.warn("bucket {} done, marking it as {}, failed shards are: {}", bucketId, status, bd.awaiting)
        return statusSyncer.syncBucket(bucketId, status, true, bd.awaiting.toSet() + bd.processing.toSet())
    }

    fun purgeIfNeeded() {
        when {
            buckets.size <= maxBuckets -> if (l.isDebugEnabled) l.debug("nothing to purge")
            else -> {
                if (l.isDebugEnabled) l.debug("initiating purge check for buckets: {}", this.buckets)
                val task = {
                    buckets.keys.sorted().take(buckets.size - maxBuckets).map { b ->
                        buckets[b]!!.let {
                            if (it.processing.cardinality() > 0) {
                                if (l.isDebugEnabled) l.debug("skipping purge of bucket {}, shards are still being processed", b)
                                immediateFuture(it)
                            } else {
                                if (l.isDebugEnabled) l.debug("purging bucket snapshot: {}", it)
                                val bs = buckets.remove(it.id)
                                when {
                                    it.count == 0L -> immediateFuture(it)
                                    it.awaiting.cardinality() == 0 -> {
                                        if (l.isDebugEnabled) l.debug("bucket {} is processed", b)
                                        statusSyncer.syncBucket(b, PROCESSED, false, bs!!.awaiting.toSet() + bs.processing.toSet()).transform { bs }
                                    }
                                    else -> {
                                        l.warn("bucket {} is marked error as final status", b)
                                        statusSyncer.syncBucket(b, ERROR, false, bs!!.awaiting.toSet() + bs.processing.toSet()).transform { bs }
                                    }
                                }
                            }
                        }
                    }.reduce()
                }
                task.retriable().done({ l.error("error in purging snapshots", it.rootCause()) }) {
                    if (l.isInfoEnabled) l.info("purged buckets: {}", it?.map { it!!.id })
                }
            }
        }
    }

}
