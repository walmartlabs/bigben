/*-
 * #%L
 * Bigben:cron
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
package com.walmartlabs.bigben.cron

import com.cronutils.descriptor.CronDescriptor
import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.mapping.annotations.Column
import com.datastax.driver.mapping.annotations.PartitionKey
import com.datastax.driver.mapping.annotations.Table
import com.datastax.driver.mapping.annotations.Transient
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY
import com.google.common.cache.CacheBuilder
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.BigBen.module
import com.walmartlabs.bigben.cron.CronRunner.crons
import com.walmartlabs.bigben.entities.Event
import com.walmartlabs.bigben.entities.EventResponse
import com.walmartlabs.bigben.entities.EventStatus.TRIGGERED
import com.walmartlabs.bigben.extns.event
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.extns.response
import com.walmartlabs.bigben.extns.save
import com.walmartlabs.bigben.extns.toResponse
import com.walmartlabs.bigben.processors.ProcessorRegistry
import com.walmartlabs.bigben.providers.domain.cassandra.ConsistencyOverride
import com.walmartlabs.bigben.providers.domain.cassandra.TTLOverride
import com.walmartlabs.bigben.utils.catching
import com.walmartlabs.bigben.utils.commons.Module
import com.walmartlabs.bigben.utils.commons.ModuleRegistry
import com.walmartlabs.bigben.utils.commons.Props
import com.walmartlabs.bigben.utils.commons.Props.int
import com.walmartlabs.bigben.utils.done
import com.walmartlabs.bigben.utils.hz.Hz
import com.walmartlabs.bigben.utils.json
import com.walmartlabs.bigben.utils.listenable
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.reduce
import com.walmartlabs.bigben.utils.rootCause
import com.walmartlabs.bigben.utils.stackTraceAsString
import com.walmartlabs.bigben.utils.transform
import com.walmartlabs.bigben.utils.transformAsync
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.DAYS
import java.time.temporal.ChronoUnit.HOURS
import java.time.temporal.ChronoUnit.MINUTES
import java.time.temporal.ChronoUnit.MONTHS
import java.time.temporal.ChronoUnit.WEEKS
import java.time.temporal.ChronoUnit.YEARS
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newScheduledThreadPool
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Created by smalik3 on 6/29/18
 */
@JsonInclude(NON_EMPTY)
data class Cron(
        val id: String, val expression: String, val type: CronType, val tenant: String,
        var lastExecutionTime: String?, val lastUpdated: String?,
        val logGranularity: ChronoUnit?, val retention: Int?, val retentionUnits: TimeUnit = TimeUnit.DAYS
) {

    init {
        require(logGranularity in supportedGranularities) { "invalid granularity: $logGranularity, only $supportedGranularities granularities are supported" }
    }

    fun fqdnCronId() = fqdnCronId(tenant, id, type)
    override fun toString() = "${fqdnCronId()}:$expression"

    companion object {
        fun fqdnCronId(tenant: String, id: String, type: CronType) = "$tenant/$id/$type"
        private val supportedGranularities = setOf(MINUTES, HOURS, DAYS, WEEKS, MONTHS, YEARS)
    }

    fun describe(locale: Locale = Locale.US) = CronDescriptor.instance(locale).describe(CronRunner.parser(type).parse(expression))!!
}

@Table(name = "cron_events")
data class CronLogEvent(
        @Transient var cron: Cron? = null,
        @PartitionKey @Column(name = "cron_id") var cronId: String? = null,
        @PartitionKey(1) @Column(name = "bucket_id") var bucketId: ZonedDateTime? = null,
        @Column(name = "event_time") var eventTime: ZonedDateTime? = null,
        var event: String? = null) : ConsistencyOverride, TTLOverride {
    override fun write() = ConsistencyLevel.valueOf(Props.string("cron.log.events.write.consistency", "ONE"))
    override fun ttl(): Int {
        val retention = cron!!.retention
        return if (retention != null) TimeUnit.SECONDS.convert(retention.toLong(), cron!!.retentionUnits).toInt() else 0
    }
}

object CronRunner : Module {

    private val l = logger<CronRunner>()
    internal val crons = module<Hz>().hz.getMap<Int, Crons>("crons")

    private val invalidExecutionTimesRetention = 1L to TimeUnit.MINUTES

    private val executionTimes = CacheBuilder.newBuilder().expireAfterAccess(invalidExecutionTimesRetention.first, invalidExecutionTimesRetention.second)
            .removalListener<String, ExecutionTime> { l.info { "removing cron from execution: ${it.key}" } }
            .build<String, ExecutionTime>()!!

    override fun init(registry: ModuleRegistry) {
        l.info("initializing the cron module: starting the cron runner(s)")
        val lastRun = AtomicReference<ZonedDateTime?>()
        val processor = module<ProcessorRegistry>()
        workers.scheduleAtFixedRate({ executionTimes.cleanUp() }, invalidExecutionTimesRetention.first,
                invalidExecutionTimesRetention.first, invalidExecutionTimesRetention.second)
        workers.scheduleAtFixedRate({
            try {
                val now = nowUTC().withNano(0)
                if (lastRun.get() == null || now > lastRun.get()) {
                    lastRun.set(now)
                    if (l.isDebugEnabled) l.debug("finding executable crons at: $now")
                    val nowString = now.toString()
                    val matches = crons.localKeySet().mapNotNull { crons[it] }.flatMap {
                        it.crons.values.filter {
                            executionTimes.get("${it.fqdnCronId()}/${it.expression}") {
                                l.info { "cron-parse: calculating execution time profile for cron: ${it.fqdnCronId()}" }
                                val parsed = parser(it.type).parse(it.expression)
                                ExecutionTime.forCron(parsed)
                            }.isMatch(now).apply {
                                if (this && l.isDebugEnabled) l.debug("cron-match: ${it.fqdnCronId()}, time: $nowString")
                            }
                        }
                    }
                    if (matches.isNotEmpty() && l.isDebugEnabled) {
                        l.debug("cron-match: matching crons at $nowString => ${matches.map { it.fqdnCronId() }}")
                    }

                    matches.map { c ->
                        val e = EventResponse(
                                c.fqdnCronId(), nowString, c.tenant, eventId = "${c.fqdnCronId()}/$nowString",
                                triggeredAt = nowString, eventStatus = TRIGGERED, payload = c.expression
                        ).event()
                        if (l.isDebugEnabled) l.debug("cron-trigger: ${c.fqdnCronId()} at $nowString")
                        processor(e).catching {
                            EventResponse(
                                    c.fqdnCronId(), nowString, c.tenant, eventId = "${c.fqdnCronId()}/$nowString",
                                    triggeredAt = nowString, eventStatus = TRIGGERED, payload = c.expression
                            ).event().apply { error = it.stackTraceAsString() }
                        }.transformAsync {
                            if (l.isDebugEnabled) {
                                l.debug { "cron-trigger-successful: ${c.fqdnCronId()} at $nowString" }
                                l.debug { "cron-execution-time-update: ${c.fqdnCronId()} at $nowString" }
                            }
                            updateCronExecutionTime(c, now, it!!)
                                    .done({ l.error(it.rootCause()) { "cron-execution-time-update-failure: ${c.fqdnCronId()} at $nowString" } }) {
                                        l.debug { "cron-execution-time-update-successful: ${c.fqdnCronId()} at $nowString" }
                                    }
                        }
                    }.reduce().done({
                        l.error { "cron-trigger-failure: there were some cron failures at $nowString" }

                    })
                    { if (matches.isNotEmpty()) l.debug { "cron-trigger-successful: all crons succeeded" } }
                }
            } catch (e: Exception) {
                l.error("error in running cron", e.rootCause()!!)
            }
        }, 0, 1, TimeUnit.SECONDS)
    }

    private val parsers = ConcurrentHashMap<CronType, CronParser>()
    internal fun parser(type: CronType) = parsers.computeIfAbsent(type) { CronParser(CronDefinitionBuilder.instanceDefinitionFor(type)) }

    private val index = AtomicInteger()
    private val workers =
            newScheduledThreadPool(int("cron.runner.core.pool.size")) { Thread(it, "cron-runner#${index.incrementAndGet()}") }

    private fun updateCronExecutionTime(cron: Cron, executionTime: ZonedDateTime, event: Event):
            ListenableFuture<Cron> {
        val f =
                crons.submitToKey(cron.partition(), CronUpdateExecutionTimeEntryProcessor(cron.fqdnCronId(), executionTime.toString()))
                        .listenable().transform { cron }
        return if (cron.logGranularity != null) {
            f.transformAsync {
                save<CronLogEvent> {
                    it.cron = cron
                    it.cronId = cron.fqdnCronId()
                    it.bucketId = executionTime.truncatedTo(cron.logGranularity)
                    it.eventTime = executionTime
                    it.event = event.toResponse().json()
                }.transform { cron }
            }
        } else f
    }
}

private fun Cron.partition() = module<Hz>().hz.partitionService.getPartition(fqdnCronId()).partitionId
private fun String.partition() = module<Hz>().hz.partitionService.getPartition(this).partitionId

object CronService {

    private val l = logger<CronService>()

    fun upsert(cron: Cron) = response {
        if (l.isInfoEnabled) l.info("creating/updating cron: $cron")
        require(cron.tenant in module<ProcessorRegistry>().registeredTenants()) { "unknown tenant: ${cron.tenant}" }
        val description = cron.describe()
        val cronId = cron.fqdnCronId()
        val pId = cron.partition()
        if (l.isDebugEnabled) l.debug("cron: $cronId hashed to partition: $pId")
        crons.executeOnKey(pId, CronEntryProcessor(cron.copy(lastUpdated = nowUTC().toString(), lastExecutionTime = null).json()))
        if (l.isDebugEnabled) l.debug("cron: $cronId updated successfully")
        CronDescription(cron, description)
    }

    fun delete(tenant: String, id: String, type: String) = response {
        val types = if (type == "*") CronType.values().toSet() else setOf(CronType.valueOf(type))
        if (l.isInfoEnabled) l.info("deleting cron: $tenant/$id, types: $types")
        types.forEach {
            val cronId = Cron.fqdnCronId(tenant, id, it)
            val pId = cronId.partition()
            if (l.isDebugEnabled) l.debug("cron: $cronId hashed to partition: $pId")
            crons.executeOnKey(pId, CronDeleteEntryProcessor(cronId))
            if (l.isDebugEnabled) l.debug("cron: $cronId deleted successfully")
        }
        mapOf("status" to "OK")
    }

    @JsonInclude(NON_EMPTY)
    data class CronDescription(val cron: Cron, val description: String?)

    fun get(tenant: String, id: String) = response {
        crons.values.flatMap { it.crons.values.filter { it.tenant == tenant && it.id == id } }
                .map { CronDescription(it, it.describe()) }
    }
}
