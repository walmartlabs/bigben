package com.walmartlabs.opensource.bigben.entities.core

import com.fasterxml.jackson.core.type.TypeReference
import com.google.common.base.Throwables.getRootCause
import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimap
import com.google.common.util.concurrent.Futures.immediateFuture
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListenableScheduledFuture
import com.google.common.util.concurrent.MoreExecutors.listeningDecorator
import com.walmartlabs.opensource.bigben.entities.Bucket
import com.walmartlabs.opensource.bigben.entities.Event
import com.walmartlabs.opensource.bigben.entities.EventLookup
import com.walmartlabs.opensource.bigben.entities.EventStatus
import com.walmartlabs.opensource.bigben.entities.EventStatus.*
import com.walmartlabs.opensource.bigben.entities.utils.Props
import com.walmartlabs.opensource.bigben.extns.*
import java.lang.Runtime.getRuntime
import java.time.ZonedDateTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Predicate
import kotlin.Comparator
import kotlin.collections.HashMap

/**
 * Created by smalik3 on 2/21/18
 */
class BucketManager(private val maxBuckets: Int, private val maxProcessingTime: Int, private val bucketWidth: Int,
                    checkpointInterval: Long, checkpointUnit: TimeUnit, private val lookbackRange: Int) {

    companion object {
        private val l = logger<BucketManager>()
        private val index = AtomicInteger()
        val scheduler = listeningDecorator(ScheduledThreadPoolExecutor(4, ThreadFactory { Thread(it, "BucketManager-${index.getAndIncrement()}") }))!!
    }

    private val shardSize = Props.int("event.shard.size", 1000)
    private val checkpointHelper = CheckpointHelper()
    private val statusSyncer = StatusSyncer()
    private val buckets = ConcurrentHashMap<ZonedDateTime, BucketSnapshot>()

    @Volatile private var bucketsLoader: BucketsLoader? = null

    init {
        l.info("saving checkpoint every {} {}", checkpointInterval, checkpointUnit)
        scheduler.scheduleAtFixedRate({ saveCheckpoint() }, checkpointInterval, checkpointInterval, checkpointUnit)
        l.info("loading the previously saved checkpoint, if any")
        try {
            checkpointHelper.loadCheckpoint().transform {
                if (l.isDebugEnabled) l.info("loaded previous checkpoint: {}")
                buckets.putAll(it!!)
                purgeIfNeeded()
            }.get()
        } catch (e: Exception) {
            l.error("could not load previous checkpoint", getRootCause(e))
        }
        getRuntime().addShutdownHook(Thread {
            l.info("saving checkpoint during shutdown")
            saveCheckpoint()
        })
    }

    internal fun getProcessableShardsForOrBefore(bucketId: ZonedDateTime): ListenableFuture<Multimap<ZonedDateTime, Int>> {
        if (bucketsLoader == null) {
            if (l.isInfoEnabled) l.info("starting the background load of previous buckets")
            val fetchSize = Props.int("buckets.background.load.fetch.size", 10)
            bucketsLoader = BucketsLoader(lookbackRange, fetchSize, Predicate { buckets.containsKey(it) }, bucketWidth, bucketId) {
                buckets.put(it.id!!, BucketSnapshot(it.id!!, it.count!!, shardSize, it.status!!))
            }.apply { run() }
        }
        return HashMultimap.create<ZonedDateTime, Int>().let { shards ->
            fetch<Bucket>(bucketId).transform {
                val bucket = it ?: Bucket(bucketId, EMPTY, 0L)
                if (buckets.putIfAbsent(bucketId, BucketSnapshot(bucketId, bucket.count!!, shardSize, bucket.status!!)) != null) {
                    l.warn("bucket with id {} already existed in the cache, this is highly unusual", bucketId)
                }
                buckets.entries.filter { e -> e.value.awaiting.cardinality() > 0 }.forEach { e -> e.value.awaiting.stream().forEach { s -> shards.put(e.key, s) } }
                l.info("processable shards at bucket: {}, are => {}", bucketId, shards)
                if (!shards.containsKey(bucketId)) {
                    l.info("no events in the bucket: {}", bucketId)
                }
                shards
            }.catching { shards.also { l.warn("error in loading bucket: {}, will be retried again during next scan", bucketId, it) } }
        }
    }

    internal fun registerForProcessing(pairs: Collection<Pair<ZonedDateTime, Int>>) {
        pairs.forEach { p -> buckets[p.first]!!.processing(p.second) }
        purgeIfNeeded()
        startShardsTimer(pairs)
    }

    private fun startShardsTimer(pairs: Collection<Pair<ZonedDateTime, Int>>): ListenableScheduledFuture<*> {
        return pairs.sortedWith(Comparator { p1, p2 ->
            p1.first.compareTo(p2.first).
                    let { if (it != 0) it else p1.second.compareTo(p2.second) }
        }).map { "${it.first}[${it.second}]" }.toList().let {
            if (l.isInfoEnabled) l.info("starting processing timer for shards: {}", it)
            scheduler.schedule({ checkShardsStatus(pairs, it) }, maxProcessingTime.toLong(), SECONDS)
        }
    }

    @Synchronized private fun checkShardsStatus(pairs: Collection<Pair<ZonedDateTime, Int>>, shards: List<String>) {
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

    @Synchronized internal fun shardDone(bucketId: ZonedDateTime, shard: Int?, status: EventStatus) {
        val bd = buckets[bucketId]
        if (bd == null) {
            l.warn("bucket {} not found in cache, might have been purged, ignoring this call", bucketId)
            return
        }
        bd.done(shard!!, status)
    }

    private val noOp = immediateFuture<Bucket>(null)

    @Synchronized internal fun bucketProcessed(bucketId: ZonedDateTime, status: EventStatus): ListenableFuture<Bucket> {
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
        return statusSyncer.syncBucket(bucketId, status, true)
    }

    @Synchronized private fun saveCheckpoint() {
        try {
            purgeIfNeeded()
            checkpointHelper.saveCheckpoint(buckets)
        } catch (e: Exception) {
            l.error("failed to save checkpoint", e)
        }
    }

    private fun purgeIfNeeded() {
        when {
            buckets.size <= maxBuckets -> if (l.isDebugEnabled) l.debug("nothing to purge")
            else -> {
                if (l.isDebugEnabled) l.debug("initiating purge check for buckets: {}", this.buckets)
                buckets.keys.sorted().take(buckets.size - maxBuckets).flatMap { b ->
                    buckets[b]!!.let {
                        when {
                            it.processing.cardinality() > 0 -> if (l.isDebugEnabled) l.debug("skipping purge of bucket {}, shards are still being processed", b)
                            it.count > 0 && it.awaiting.cardinality() == 0 -> {
                                if (l.isDebugEnabled) l.debug("bucket {} is processed", b)
                                return@flatMap listOf(statusSyncer.syncBucket(b, PROCESSED, false))
                            }
                            it.count > 0 && it.awaiting.cardinality() != 0 -> {
                                l.warn("bucket {} is marked error as final status", b)
                                return@flatMap listOf(statusSyncer.syncBucket(b, ERROR, false))
                            }
                        }
                        emptyList<ListenableFuture<Bucket>>()
                    }
                }.reduce().done({ l.error("error in purging snapshots", getRootCause(it!!)) }) {
                    if (l.isInfoEnabled) l.info("purged buckets: {}", it?.map { it.id })
                }
            }
        }
    }

    private data class BucketSnapshot(val id: ZonedDateTime, val count: Long, val processing: BitSet, val awaiting: BitSet, private val shards: Int = -1) {

        companion object {
            private val l = logger<BucketSnapshot>()
            private val EMPTY = BitSet()
        }

        constructor(id: ZonedDateTime, count: Long, shardSize: Int, status: EventStatus) : this(id, count, BitSet(),
                if (count == 0L || PROCESSED == status) EMPTY else {
                    val shards = (if (count % shardSize == 0L) count / shardSize else count / shardSize + 1).toInt()
                    BitSet(shards).apply { set(0, shards) }
                }, (if (count % shardSize == 0L) count / shardSize else count / shardSize + 1).toInt())

        init {
            when {
                count == 0L -> l.info("bucket: {} => empty, no events", id)
                awaiting === EMPTY -> l.info("bucket: {} => already done", id)
                else -> {
                    l.info("bucket: {} => has {} events, resulting in {} shards", id, count, shards)
                }
            }
        }

        fun processing(shard: Int) = apply { awaiting.clear(shard); processing.set(shard) }

        fun done(shard: Int, status: EventStatus) {
            processing.clear(shard)
            when (status) {
                PROCESSED -> {
                    l.info("shard: {}[{}] finished successfully", id, shard)
                    awaiting.clear(shard)
                }
                ERROR -> {
                    l.info("shard: {}[{}] finished with error", id, shard)
                    awaiting.set(shard)
                }
                else -> throw IllegalArgumentException("invalid status value: $status")
            }
        }
    }

    private class CheckpointHelper {
        companion object {
            private val l = logger<CheckpointHelper>()
            private val CHECKPOINT_KEY = "_CHECKPOINT_"
        }

        fun saveCheckpoint(data: Map<ZonedDateTime, BucketSnapshot>): ListenableFuture<EventLookup> {
            return data.toSortedMap().mapValues {
                mapOf("b" to it.value.id.toString(), "c" to it.value.count,
                        "a" to it.value.awaiting.stream().boxed().toArray().joinToString(","),
                        "p" to it.value.processing.stream().boxed().toArray().joinToString(","))
            }.let {
                if (l.isDebugEnabled) {
                    l.debug("saving checkpoint for buckets: ${it.keys}")
                }
                EventLookup(CHECKPOINT_KEY, CHECKPOINT_KEY, "", -1, nowUTC(), "", it.values.json()).save()
            }.done {
                if (l.isDebugEnabled && it != null)
                    l.debug("checkpoint saved successfully for buckets: ${it.payload}")
            }
        }

        fun loadCheckpoint(): ListenableFuture<Map<ZonedDateTime, BucketSnapshot>> {
            return DOManager.fetch(CHECKPOINT_KEY, CHECKPOINT_KEY).transform {
                when {
                    it?.payload != null && it.payload.trim().isNotEmpty() -> {
                        object : TypeReference<List<Map<String, Any>>>() {}.fromJson(it.payload).map { it.mapValues { it.value.toString() } }.map {
                            val awaiting = it["a"]?.let { if (it.isEmpty()) BitSet() else it.split(",").fold(BitSet()) { b, s -> b.set(s.toInt()); b } } ?: BitSet()
                            val processing = it["p"]?.let { if (it.isEmpty()) BitSet() else it.split(",").fold(BitSet()) { b, s -> b.set(s.toInt()); b } } ?: BitSet()
                            processing.stream().forEach { awaiting.set(it) }
                            BucketSnapshot(ZonedDateTime.parse(it["a"].toString()), it["c"].toString().toLong(), awaiting, BitSet())
                        }.associate { it.id to it }.also { if (l.isDebugEnabled) l.debug("loaded checkpoint: {}", it) }
                    }
                    else -> {
                        l.info("no checkpoint to load"); HashMap()
                    }
                }
            }.catching { l.error("error in loading checkpoint, ignoring", it); HashMap() }
        }
    }

    private class StatusSyncer {
        companion object {
            private val l = logger<StatusSyncer>()
        }

        fun syncBucket(bucketId: ZonedDateTime, status: EventStatus, setProcessedAt: Boolean): ListenableFuture<Bucket> {
            Bucket(status = status).also { if (setProcessedAt) it.processedAt = nowUTC() }
            if (l.isDebugEnabled)
                l.debug("bucket {} is done, syncing status as {}", bucketId, status)
            return save<Bucket> {
                it.status = status
                if (setProcessedAt) it.processedAt = nowUTC()
            }.done({ l.error("bucket {} could not be synced as {}, after multiple retries", bucketId, status, it) })
            { l.info("bucket {} is successfully synced as {}", bucketId, status) }
        }

        fun syncShard(bucketId: ZonedDateTime, shard: Int, eventTime: ZonedDateTime, eventId: String, status: EventStatus, payload: String?): ListenableFuture<Event> {
            if (l.isDebugEnabled)
                l.info("shard {}[{}] is done, syncing status as {}, payload: {}", bucketId, shard, status, payload)
            return save<Event> {
                it.status = status
                if (payload != null)
                    it.payload = payload
            }.done({ l.error("shard {}[{}] could not be synced with status {}, after multiple retries", bucketId, shard, status, it) }) {
                l.info("shard {}[{}] is successfully synced with status {}", bucketId, shard, status)
            }
        }
    }
}