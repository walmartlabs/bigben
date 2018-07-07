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
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.BigBen.hz
import com.walmartlabs.bigben.BigBen.processorRegistry
import com.walmartlabs.bigben.cron.CronRunner.crons
import com.walmartlabs.bigben.entities.Event
import com.walmartlabs.bigben.entities.EventResponse
import com.walmartlabs.bigben.entities.EventStatus
import com.walmartlabs.bigben.entities.KV
import com.walmartlabs.bigben.extns.*
import com.walmartlabs.bigben.utils.*
import com.walmartlabs.bigben.utils.commons.Module
import com.walmartlabs.bigben.utils.commons.Props.int
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.*
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newScheduledThreadPool
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import javax.ws.rs.*
import javax.ws.rs.core.MediaType.APPLICATION_JSON

/**
 * Created by smalik3 on 6/29/18
 */
@JsonInclude(NON_EMPTY)
data class Cron(val id: String, val expression: String, val type: CronType, val tenant: String,
                var lastExecutionTime: ZonedDateTime?, val lastUpdated: ZonedDateTime?,
                val tracingEnabled: Boolean = false, val tracingGranularity: ChronoUnit = DAYS) {

    private val computed = ConcurrentHashMap<Int, Any>()

    private fun parsed() = computed.computeIfAbsent(0) { CronRunner.parser(type).parse(expression)!! } as com.cronutils.model.Cron
    internal fun executionTime() = computed.computeIfAbsent(1) { println("computing"); ExecutionTime.forCron(parsed()) } as ExecutionTime

    init {
        require(tracingGranularity in supportedGranularities) { "only $supportedGranularities granularities are supported" }
    }

    fun cronId() = cronId(tenant, id, type)
    override fun toString() = "${cronId()}:$expression"

    companion object {
        fun cronId(tenant: String, id: String, type: CronType) = "$tenant/$id/$type"
        private val supportedGranularities = setOf(ChronoUnit.SECONDS, MINUTES, HOURS, DAYS, WEEKS, MONTHS, YEARS)
    }

    fun toGranularity(zdt: ZonedDateTime): String {
        return when (tracingGranularity) {
            YEARS -> zdt.year.toString()
            MONTHS -> "${zdt.year}/${zdt.monthValue}"
            WEEKS -> "${zdt.year}/${zdt.monthValue}/${WEEKS.between(zdt.withDayOfMonth(1), zdt)}"
            DAYS -> "${zdt.year}/${zdt.dayOfYear}"
            HOURS -> "${zdt.year}/${zdt.dayOfYear}/${zdt.hour}"
            MINUTES -> "${zdt.year}/${zdt.dayOfYear}/${zdt.hour}/${zdt.minute}"
            SECONDS -> "${zdt.year}/${zdt.dayOfYear}/${zdt.hour}/${zdt.minute}/${zdt.second}"
            else -> throw IllegalArgumentException("unsupported unit: $tracingGranularity")
        }
    }

    fun describe(locale: Locale = Locale.US) = CronDescriptor.instance(locale).run { describe(parsed()) }!!
}

object CronRunner : Module {

    private val l = logger<CronRunner>()
    internal val crons = hz.hz.getMap<Int, Crons>("crons")

    override fun init() {
        l.info("initializing the cron module: starting the cron runner(s)")
        val lastRun = AtomicReference<ZonedDateTime?>()
        workers.scheduleAtFixedRate({
            try {
                val now = nowUTC().withNano(0)
                if (lastRun.get() == null || now > lastRun.get()) {
                    lastRun.set(now)
                    val nowString = now.toString()
                    @Suppress("UNCHECKED_CAST")
                    val matches = (crons.executeOnKeys(crons.localKeySet(), CronMatchExecutionTimeProcessor(
                            now.toInstant().toEpochMilli())) as MutableMap<Int, List<String>>).values.flatten().map { Cron::class.java.fromJson(it) }
                    if (matches.isNotEmpty()) {
                        matches.map { c ->
                            val e = EventResponse(c.id, nowString, c.tenant, eventId = "${c.type}/$nowString",
                                    triggeredAt = nowString, eventStatus = EventStatus.TRIGGERED, payload = c.expression).event()
                            if (l.isDebugEnabled) l.debug("triggering event for cron: ${c.cronId()} at $nowString")
                            processorRegistry(e).transformAsync { updateCronExecutionTime(c, now, it!!) }
                        }.reduce().done({ l.error("cron-failed: time: $nowString, crons: ${matches.map { it.cronId() }}") })
                        { if (l.isDebugEnabled) l.debug("cron-successful: time: $nowString, crons: ${matches.map { it.cronId() }}") }
                    }
                }
            } catch (e: Exception) {
                l.error("error in running cron", e.rootCause()!!)
            }
        }, 0, 1, SECONDS)
    }

    private val parsers = ConcurrentHashMap<CronType, CronParser>()
    internal fun parser(type: CronType) = parsers.computeIfAbsent(type) { CronParser(CronDefinitionBuilder.instanceDefinitionFor(type)) }

    private val index = AtomicInteger()
    private val workers = newScheduledThreadPool(int("cron.runner.core.pool.size")) { Thread(it, "cron-runner#${index.incrementAndGet()}") }

    private fun updateCronExecutionTime(cron: Cron, executionTime: ZonedDateTime, event: Event): ListenableFuture<Cron> {
        val f = crons.submitToKey(cron.partition(), CronUpdateExecutionTimeEntryProcessor(cron.cronId(), executionTime.toString())).listenable().transform { cron }
        return if (cron.tracingEnabled) {
            f.transformAsync {
                save<KV> {
                    it.key = "${cron.cronId()}:${cron.toGranularity(executionTime)}"
                    it.column = executionTime.toString(); it.value = event.toResponse().yaml()
                }.transform { cron }
            }
        } else f
    }
}

private fun Cron.partition() = hz.hz.partitionService.getPartition(cronId()).partitionId
private fun String.partition() = hz.hz.partitionService.getPartition(this).partitionId

@Path("/cron")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
object CronService {

    private val l = logger<CronService>()

    @POST
    fun upsert(cron: Cron) = response {
        if (l.isInfoEnabled) l.info("creating/updating cron: $cron")
        val cronId = cron.cronId()
        val pId = cron.partition()
        if (l.isDebugEnabled) l.debug("cron: $cronId hashed to partition: $pId")
        crons.executeOnKey(pId, CronEntryProcessor(cron.copy(lastUpdated = nowUTC(), lastExecutionTime = null).json()))
        if (l.isDebugEnabled) l.debug("cron: $cronId updated successfully")
        mapOf("status" to "OK")
    }

    @DELETE
    @Path("/{tenant}/{id}/{type}")
    fun delete(@PathParam("tenant") tenant: String, @PathParam("id") id: String, @PathParam("type") type: String) = response {
        val types = if (type == "*") CronType.values().toSet() else setOf(CronType.valueOf(type))
        if (l.isInfoEnabled) l.info("deleting cron: $tenant/$id, types: $types")
        types.forEach {
            val cronId = Cron.cronId(tenant, id, it)
            val pId = cronId.partition()
            if (l.isDebugEnabled) l.debug("cron: $cronId hashed to partition: $pId")
            crons.executeOnKey(pId, CronDeleteEntryProcessor(cronId))
            if (l.isDebugEnabled) l.debug("cron: $cronId deleted successfully")
        }
        mapOf("status" to "OK")
    }

    @JsonInclude(NON_EMPTY)
    data class CronDescription(val cron: Cron, val description: String?)

    @GET
    @Path("/{tenant}/{id}")
    fun get(@PathParam("tenant") tenant: String, @PathParam("id") id: String, @QueryParam("describe") describe: Boolean?) = response {
        crons.values.flatMap { it.crons.values.filter { it.tenant == tenant && it.id == id } }.map { CronDescription(it, describe?.run { it.describe() }) }
    }

    @GET
    @Path("/describe")
    fun describe(cron: Cron) = response {
        CronDescription(cron, cron.describe())
    }
}
