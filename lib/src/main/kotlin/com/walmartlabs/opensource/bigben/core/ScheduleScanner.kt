package com.walmartlabs.opensource.bigben.core

import com.google.common.base.Throwables.getRootCause
import com.google.common.collect.Iterators
import com.google.common.collect.LinkedHashMultimap
import com.google.common.collect.Multimap
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors.listeningDecorator
import com.hazelcast.core.IExecutorService
import com.hazelcast.core.Member
import com.walmartlabs.opensource.bigben.entities.EventStatus.ERROR
import com.walmartlabs.opensource.bigben.entities.ShardStatus
import com.walmartlabs.opensource.bigben.entities.ShardStatusList
import com.walmartlabs.opensource.bigben.extns.*
import com.walmartlabs.opensource.bigben.hz.Hz
import com.walmartlabs.opensource.bigben.hz.Service
import com.walmartlabs.opensource.bigben.tasks.BulkShardTask
import com.walmartlabs.opensource.bigben.utils.Props
import java.lang.Runtime.getRuntime
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.MINUTES
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Created by smalik3 on 2/23/18
 */
class ScheduleScanner(private val hz: Hz) : Service {

    companion object {
        internal var BUCKET_CACHE = "bucketCache"
        val EVENT_SCHEDULER = "event_scheduler"

        private val l = logger<ScheduleScanner>()

        private val index = AtomicInteger()
        private val scheduler = listeningDecorator(ScheduledThreadPoolExecutor(getRuntime().availableProcessors()
        ) { r -> Thread(r, "InternalScheduler#" + index.getAndIncrement()) })
    }

    private val isShutdown = AtomicReference(false)

    private lateinit var bucketManager: BucketManager
    private var bucketWidth = Props.int("event.schedule.scan.interval.minutes", 1)
    @Volatile private lateinit var lastScan: ZonedDateTime

    override val name: String = "ScheduleScanner"

    override fun init() {
        if (l.isInfoEnabled) l.info("initing the event scheduler")
        val lookbackRange = Props.int("events.backlog.check.limit", 2)
        bucketWidth = Props.int("event.schedule.scan.interval.minutes", 1)
        val checkpointInterval = Props.long("event.bucket.manager.checkpoint.interval", 1)
        val checkpointIntervalUnits = TimeUnit.valueOf(Props.string("event.bucket.manager.checkpoint.interval.units", MINUTES.name))
        bucketManager = BucketManager(lookbackRange, 2 * bucketWidth * 60, bucketWidth * 60, checkpointInterval, checkpointIntervalUnits, lookbackRange)
    }

    override fun execute() {
        if (l.isInfoEnabled) l.info("executing the EventScheduleScanner")
        val scanInterval = Props.int("event.schedule.scan.interval.minutes", 1)
        if (l.isInfoEnabled) l.info("calculating the next scan bucketId")
        val now = nowUTC()
        val nextScan = nextScan(now, scanInterval)
        val delay = ChronoUnit.MILLIS.between(now, nextScan)
        val bucket = ZonedDateTime.ofInstant(Instant.ofEpochMilli(bucketize(now.toInstant().toEpochMilli(), scanInterval)), UTC)
        lastScan = bucket.minusMinutes(bucketWidth.toLong())
        if (l.isInfoEnabled) l.info("first-scan at: {}, for bucket: {}, next-scan at: {}, " + "initial-delay: {} ms, subsequent-scans: after every {} minutes", now, bucket, nextScan, delay, scanInterval)
        scheduler.scheduleAtFixedRate({ this.scan() }, delay, MILLISECONDS.convert(scanInterval.toLong(), MINUTES), MILLISECONDS)
        if (l.isInfoEnabled) l.info("executing first time scan")
        scan()
    }

    private fun scan() {
        if (isShutdown.get()) {
            if (l.isInfoEnabled) l.info("system is shutdown, no more schedules will be processed")
            return
        }
        val currentBucketId = lastScan.plusMinutes(bucketWidth.toLong())
        lastScan = currentBucketId
        if (l.isInfoEnabled) l.info("scanning the schedule(s) for bucket: {}", currentBucketId)
        try {
            bucketManager.getProcessableShardsForOrBefore(currentBucketId).
                    done({ l.error("error in processing bucket: {}", currentBucketId, it!!.rootCause()) }) {
                        try {
                            if (it!!.isEmpty) {
                                if (l.isInfoEnabled) l.info("nothing to schedule for bucket: " + currentBucketId)
                                return@done
                            }
                            if (l.isInfoEnabled) l.info("%s, shards to be processed: => %s", currentBucketId, it)
                            val distro = calculateDistro(it)
                            if (l.isInfoEnabled) l.info("{}, schedule distribution: => {}", currentBucketId, distro)

                            val map = distro.asMap()
                            val iterator = Iterators.cycle<Member>(map.keys)
                            val executorService = hz.hz.getExecutorService(EVENT_SCHEDULER)
                            map.entries.map {
                                { submitShards(executorService, iterator.next(), it.value, currentBucketId) }.
                                        retriable("shards-submit", Props.int("event.submit.max.retries", 10),
                                                Props.int("event.submit.initial.delay", 1),
                                                Props.int("event.submit.backoff.multiplier", 1)).transform { it!!.list }
                            }.reduce().done({ l.error("schedule for bucket {} finished abnormally", currentBucketId, it.rootCause()) }) {
                                if (l.isInfoEnabled) l.info("schedule for bucket {} finished normally => {}", currentBucketId, it)
                                val buckets = it!!.map { it!! }.flatten().filterNotNull()
                                buckets.let { it.toSet().map { bucketManager.bucketProcessed(it.bucketId!!, it.status!!) } }.
                                        done({ l.error("bucket-scan: {}, failed to update the scan-status: {}", currentBucketId, buckets.map { it.bucketId }, it.rootCause()) }) {
                                            if (l.isInfoEnabled) l.info("bucket-scan: {}, successfully updated the scan-status: {}", currentBucketId, buckets.map { it.bucketId })
                                        }
                            }
                        } catch (e: Exception) {
                            l.error("error in processing bucket: {}", currentBucketId, getRootCause(e))
                        }
                    }
        } catch (e: Exception) {
            l.error("error in processing bucket: {}", currentBucketId, getRootCause(e))
        }
    }

    private fun calculateDistro(shards: Multimap<ZonedDateTime, Int>): LinkedHashMultimap<Member, Pair<ZonedDateTime, Int>> {
        val members = hz.hz.cluster.members.toMutableSet().apply { remove(hz.hz.cluster.localMember) }.toList().shuffled().toMutableList().apply { add(hz.hz.cluster.localMember) }
        val entries = shards.entries().toList()
        return LinkedHashMultimap.create<Member, Pair<ZonedDateTime, Int>>().apply {
            val size = members.size
            for (i in entries.indices) {
                val e = entries[i]
                put(members[i % size], e.key to e.value)
            }
        }
    }

    private fun submitShards(executorService: IExecutorService, member: Member, shardsData: Collection<Pair<ZonedDateTime, Int>>, bucket: ZonedDateTime): ListenableFuture<ShardStatusList> {
        if (l.isInfoEnabled) l.info("{}, submitting  for execution to member {}, shards: {}", bucket, member.socketAddress, shardsData)
        bucketManager.registerForProcessing(shardsData)
        return executorService.submitToMember(BulkShardTask(shardsData), member).listenable().
                catching { ShardStatusList(shardsData.map { ShardStatus(it.first, it.second, ERROR) }) }.
                done({
                    l.error("{}, member {} finished abnormally for shards: {}", bucket, member.socketAddress, shardsData, it)
                    shardsData.forEach { bucketManager.shardDone(it.first, it.second, ERROR) }
                }) {
                    l.info("{}, member {} finished normally for shards: {}", bucket, member.socketAddress, it)
                    it?.list?.forEach { bucketManager.shardDone(it!!.bucketId!!, it.shard!!, it.status!!) }
                }
    }

    override fun destroy() {
        if (l.isInfoEnabled) l.info("destroying the event scheduler")
    }
}