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

import com.google.common.base.Throwables.getRootCause
import com.google.common.collect.Iterators
import com.google.common.collect.LinkedHashMultimap
import com.google.common.collect.Multimap
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors.listeningDecorator
import com.hazelcast.core.IExecutorService
import com.hazelcast.core.Member
import com.walmartlabs.bigben.entities.EventStatus.ERROR
import com.walmartlabs.bigben.entities.EventStatus.PROCESSED
import com.walmartlabs.bigben.entities.ShardStatus
import com.walmartlabs.bigben.entities.ShardStatusList
import com.walmartlabs.bigben.extns.bucketize
import com.walmartlabs.bigben.extns.nextScan
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.tasks.BulkShardTask
import com.walmartlabs.bigben.utils.*
import com.walmartlabs.bigben.utils.hz.Hz
import com.walmartlabs.bigben.utils.hz.Service
import com.walmartlabs.bigben.utils.utils.Props
import com.walmartlabs.bigben.utils.utils.Props.int
import com.walmartlabs.bigben.utils.utils.Props.string
import java.lang.Runtime.getRuntime
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Created by smalik3 on 2/23/18
 */
class ScheduleScanner(private val hz: Hz) : Service {

    companion object {
        internal const val BUCKET_CACHE = "bucketCache"
        const val EVENT_SCHEDULER = "event_scheduler"

        private val l = logger<ScheduleScanner>()

        private val index = AtomicInteger()
        private val shardIndexer = AtomicInteger()
        private val scheduler = listeningDecorator(ScheduledThreadPoolExecutor(getRuntime().availableProcessors()) { r -> Thread(r, "InternalScheduler#" + index.getAndIncrement()) })
        private val shardSubmitters = listeningDecorator(newFixedThreadPool(int("events.num.shard.submitters")) { r -> Thread(r, "ShardSubmitter#" + shardIndexer.getAndIncrement()) })
    }

    private val isShutdown = AtomicReference(false)

    private lateinit var bucketManager: BucketManager
    private val bucketWidth = int("events.schedule.scan.interval.minutes")
    @Volatile
    private lateinit var lastScan: ZonedDateTime

    override val name: String = "ScheduleScanner"

    override fun init() {
        if (l.isInfoEnabled) l.info("initing the event scheduler")
        val lookbackRange = int("buckets.backlog.check.limit")
        val checkpointInterval = Props.long("buckets.checkpoint.interval")
        val checkpointIntervalUnits = valueOf(string("buckets.checkpoint.interval.units"))
        bucketManager = BucketManager(lookbackRange + 1, 2 * bucketWidth * 60, bucketWidth * 60, checkpointInterval, checkpointIntervalUnits, hz)
    }

    override fun execute() {
        if (l.isInfoEnabled) l.info("executing the EventScheduleScanner")
        val scanInterval = int("events.schedule.scan.interval.minutes")
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
        if (l.isDebugEnabled) l.debug("scanning the schedule(s) for bucket: {}", currentBucketId)
        try {
            bucketManager.getProcessableShardsForOrBefore(currentBucketId).done({ l.error("error in processing bucket: {}", currentBucketId, it!!.rootCause()) }) {
                try {
                    if (it!!.isEmpty) {
                        if (l.isDebugEnabled) l.debug("nothing to schedule for bucket: {}", currentBucketId)
                        return@done
                    }
                    shardSubmitters.submit {
                        if (l.isDebugEnabled) l.debug("{}, shards to be processed: => {}", currentBucketId, it)
                        calculateDistro(it).asMap().run {
                            if (l.isInfoEnabled) l.info("{}, schedule distribution: => {}", currentBucketId,
                                    mapKeys { it.key.address.toString() }.mapValues { it.value.joinToString(",") { "${it.first}[${it.second}]" } }.toSortedMap())

                            val iterator = Iterators.cycle<Member>(keys)
                            val executorService = hz.hz.getExecutorService(EVENT_SCHEDULER)
                            entries.map {
                                { submitShards(executorService, iterator.next(), it.value, currentBucketId) }.retriable("shards-submit", int("event.submit.max.retries"),
                                        int("events.submit.initial.delay"),
                                        int("events.submit.backoff.multiplier")).transform { it!!.list }
                            }.reduce().done({ l.error("schedule for bucket {} finished abnormally", currentBucketId, it.rootCause()) }) {
                                if (l.isDebugEnabled) l.debug("schedule for bucket {} finished normally => {}", currentBucketId, it)
                                val buckets = it!!.map { it!! }.flatten().filterNotNull().groupBy { it.bucketId!! }.mapValues { it.value.fold(false) { hasError, ss -> hasError || (ss.status == ERROR) } }
                                if (l.isDebugEnabled) l.debug("bucket-scan: {}, final buckets with statuses to be persisted: {}", currentBucketId, buckets)
                                buckets.map { bucketManager.bucketProcessed(it.key, if (it.value) ERROR else PROCESSED) }.done({ l.error("bucket-scan: {}, failed to update the scan-status: {}", currentBucketId, buckets.keys, it.rootCause()) }) {
                                    if (l.isDebugEnabled) l.debug("bucket-scan: {}, successfully updated the scan-status: {}", currentBucketId, buckets.keys)
                                }
                            }
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
        if (l.isDebugEnabled) l.debug("{}, submitting  for execution to member {}, shards: {}", bucket, member.socketAddress, shardsData)
        bucketManager.registerForProcessing(shardsData)
        return executorService.submitToMember(BulkShardTask(shardsData), member).listenable().catching { ShardStatusList(shardsData.map { ShardStatus(it.first, it.second, ERROR) }) }.done({
            l.error("{}, member {} finished abnormally for shards: {}", bucket, member.socketAddress, shardsData, it)
            shardsData.forEach { bucketManager.shardDone(it.first, it.second, ERROR) }
        }) {
            if (l.isDebugEnabled) l.debug("{}, member {} finished normally for shards: {}", bucket, member.socketAddress, it)
            it?.list?.forEach { bucketManager.shardDone(it!!.bucketId!!, it.shard!!, it.status!!) }
        }
    }

    override fun destroy() {
        if (l.isInfoEnabled) l.info("destroying the event scheduler")
    }
}
