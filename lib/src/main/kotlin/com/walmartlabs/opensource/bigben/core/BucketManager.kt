package com.walmartlabs.opensource.bigben.core

import com.google.common.base.Throwables.getRootCause
import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimap
import com.google.common.util.concurrent.Futures.immediateFuture
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListenableScheduledFuture
import com.google.common.util.concurrent.MoreExecutors.listeningDecorator
import com.walmartlabs.opensource.bigben.entities.Bucket
import com.walmartlabs.opensource.bigben.entities.EventStatus
import com.walmartlabs.opensource.bigben.entities.EventStatus.*
import com.walmartlabs.opensource.bigben.extns.domainProvider
import com.walmartlabs.opensource.bigben.extns.fetch
import com.walmartlabs.opensource.core.*
import com.walmartlabs.opensource.core.utils.Props
import java.lang.Runtime.getRuntime
import java.time.ZonedDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Predicate
import kotlin.Comparator

/**
 * Created by smalik3 on 2/21/18
 */
class BucketManager(private val maxBuckets: Int, private val maxProcessingTime: Int, private val bucketWidth: Int,
                    checkpointInterval: Long, checkpointUnit: TimeUnit) {

    companion object {
        private val l = logger<BucketManager>()
        private val index = AtomicInteger()
        val scheduler = listeningDecorator(ScheduledThreadPoolExecutor(4, ThreadFactory { Thread(it, "BucketManager-${index.getAndIncrement()}") }))!!

        internal fun emptyBucket(bucketId: ZonedDateTime) = domainProvider<Bucket>().let { it.raw(it.selector(Bucket::class.java)).apply { this.bucketId = bucketId; count = 0L; status = EMPTY } }
    }

    private val shardSize = Props.int("event.shard.size", 1000)
    private val checkpointHelper = CheckpointHelper()
    private val statusSyncer = StatusSyncer()
    private val buckets = ConcurrentHashMap<ZonedDateTime, BucketSnapshot>()

    @Volatile private var bucketsLoader: BucketsLoader? = null

    init {
        if (l.isDebugEnabled) l.debug("saving checkpoint every {} {}", checkpointInterval, checkpointUnit)
        scheduler.scheduleAtFixedRate({ saveCheckpoint() }, checkpointInterval, checkpointInterval, checkpointUnit)
        if (l.isDebugEnabled) l.debug("loading the previously saved checkpoint, if any")
        try {
            checkpointHelper.loadCheckpoint().transform {
                if (l.isDebugEnabled) l.debug("loaded previous checkpoint: {}")
                buckets.putAll(it!!)
                purgeIfNeeded()
            }.get()
        } catch (e: Exception) {
            l.error("could not load previous checkpoint", getRootCause(e))
        }
        getRuntime().addShutdownHook(Thread {
            if (l.isInfoEnabled) l.info("saving checkpoint during shutdown")
            saveCheckpoint()
        })
    }

    fun getProcessableShardsForOrBefore(bucketId: ZonedDateTime): ListenableFuture<out Multimap<ZonedDateTime, Int>> {
        if (bucketsLoader == null) {
            if (l.isInfoEnabled) l.info("starting the background load of previous buckets")
            val fetchSize = Props.int("buckets.background.load.fetch.size", 10)
            bucketsLoader = BucketsLoader(maxBuckets - 1, fetchSize, Predicate { buckets.containsKey(it) }, bucketWidth, bucketId) {
                buckets.put(it.bucketId!!, BucketSnapshot.with(it.bucketId!!, it.count!!, shardSize, it.status!!))
            }.apply { run() }
        }
        return HashMultimap.create<ZonedDateTime, Int>().let { shards ->
            fetch<Bucket> { it.bucketId = bucketId }.transform {
                val bucket = it ?: emptyBucket(bucketId)
                if (buckets.putIfAbsent(bucketId, BucketSnapshot.with(bucketId, bucket.count!!, shardSize, bucket.status!!)) != null) {
                    l.warn("bucket with bucketId {} already existed in the cache, this is highly unusual", bucketId)
                }
                buckets.entries.filter { e -> e.value.awaiting.cardinality() > 0 }.forEach { e -> e.value.awaiting.stream().forEach { s -> shards.put(e.key, s) } }
                if (l.isDebugEnabled) l.debug("processable shards at bucket: {}, are => {}", bucketId, shards)
                if (!shards.containsKey(bucketId)) {
                    if (l.isDebugEnabled) l.debug("no events in the bucket: {}", bucketId)
                }
                shards
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
            p1.first.compareTo(p2.first).
                    let { if (it != 0) it else p1.second.compareTo(p2.second) }
        }).map { "${it.first}[${it.second}]" }.toList().let {
            if (l.isDebugEnabled) l.debug("starting processing timer for shards: {}", it)
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
        } catch (e: Throwable) {
            l.error("failed to save checkpoint", e)
        }
    }

    fun purgeIfNeeded() {
        when {
            buckets.size <= maxBuckets -> if (l.isDebugEnabled) l.debug("nothing to purge")
            else -> {
                if (l.isDebugEnabled) l.debug("initiating purge check for buckets: {}", this.buckets)
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
                                    statusSyncer.syncBucket(b, PROCESSED, false).transform { bs }
                                }
                                else -> {
                                    l.warn("bucket {} is marked error as final status", b)
                                    statusSyncer.syncBucket(b, ERROR, false).transform { bs }
                                }
                            }
                        }
                    }
                }.reduce().done({ l.error("error in purging snapshots", getRootCause(it!!)) }) {
                    if (l.isInfoEnabled) l.info("purged buckets: {}", it?.map { it!!.id })
                }
            }
        }
    }

}