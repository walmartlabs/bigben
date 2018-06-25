package com.walmartlabs.bigben.api

import com.google.common.base.Throwables.getStackTraceAsString
import com.walmartlabs.bigben.BigBen.processorRegistry
import com.walmartlabs.bigben.entities.*
import com.walmartlabs.bigben.entities.EventStatus.REJECTED
import com.walmartlabs.bigben.entities.EventStatus.TRIGGERED
import com.walmartlabs.bigben.extns.*
import com.walmartlabs.bigben.processors.ProcessorConfig
import com.walmartlabs.bigben.tasks.StatusTask
import com.walmartlabs.bigben.utils.*
import com.walmartlabs.bigben.utils.hz.Hz
import com.walmartlabs.bigben.utils.hz.Service
import java.io.Serializable
import java.time.LocalDateTime
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeParseException
import java.util.*
import java.util.concurrent.Callable
import javax.ws.rs.*
import javax.ws.rs.core.MediaType.APPLICATION_JSON
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.Status.*
import javax.ws.rs.core.Response.ok
import javax.ws.rs.core.Response.status

/**
 * Created by smalik3 on 2/27/18
 */
@Path("/events")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
class EventService(private val hz: Hz, private val service: Service,
                   private val receiver: EventReceiver) {

    companion object {
        private val l = logger<EventService>()
        val DEBUG_FLAG = ThreadLocal<Boolean>()
    }

    @GET
    @Path("/cluster")
    fun clusterStats() = response {
        hz.hz.getExecutorService("default").submitToAllMembers(StatusTask(service.name)).mapKeys { it.key.address.toString() }.mapValues { it.value.result { "Error: ${getStackTraceAsString(it)}" } }
    }

    @POST
    @Path("/schedule")
    fun schedule(events: List<EventRequest>) = response {
        events.map { if (it.mode == Mode.UPSERT) receiver.addEvent(it) else receiver.removeEvent(it.id!!, it.tenant!!) }
                .reduce().result { emptyList() }.run {
                    filter { it.eventStatus == TRIGGERED }.map { processorRegistry(it.event()) }.done({ l.error("error in triggering lapsed events:", it.rootCause()) }) {
                        it!!.forEach {
                            l.warn("event was triggered immediately (likely lapsed), event bucketId: {}, tenant: {}, " +
                                    "eventTime: {}, currentTime: {}", it.xrefId, it.tenant, it.eventTime, nowUTC())
                        }
                    }
                    count { it.eventStatus == REJECTED }.let {
                        when {
                            it == events.size -> status(BAD_REQUEST).entity(this)
                            it > 0 -> status(PARTIAL_CONTENT).entity(this)
                            else -> ok().entity(this)
                        }
                    }
                }
    }

    @POST
    @Path("/tenant/register")
    fun registerProcessor(config: ProcessorConfig) = response {
        if (l.isInfoEnabled) l.info("saving the tenant config: $config")
        require(config.tenant != null) { "tenant must not be null" }
        save<KV> { it.key = "tenants"; it.column = config.tenant; it.value = config.json() }
        if (l.isInfoEnabled) l.info("broadcasting the tenant config to all members: $config")
        hz.hz.getExecutorService("default").submitToAllMembers(ProcessRegisterTask(config)).mapValues { it.value.listenable() }.values.toList().reduce().result { throw RuntimeException("") }
        processorRegistry.registeredConfigs()
    }

    @GET
    @Path("/tenants")
    fun registeredTenants() = response { processorRegistry.registeredConfigs() }

    @GET
    @Path("/find")
    fun find(@QueryParam("bucketId") id: String, @QueryParam("tenant") tenant: String) = response { EventRequest().apply { this.id = id; this.tenant = tenant }.let { find(it, false) } }

    @POST
    @Path("/dryrun")
    fun dryrun(@QueryParam("bucketId") id: String, @QueryParam("tenant") tenant: String) = response { EventRequest().apply { this.id = id; this.tenant = tenant }.let { find(it, true) } }

    private fun find(eventRequest: EventRequest, fire: Boolean) = response m@{
        val eventResponse = eventRequest.toResponse()
        return@m if (eventRequest.id != null && eventRequest.id!!.trim().isNotEmpty()) {
            val eventLookup = fetch<EventLookup> { it.xrefId = eventRequest.id; it.tenant = eventRequest.tenant }.result { null }
            if (eventLookup == null) {
                status(NOT_FOUND).entity(eventResponse)
            } else {
                val event = fetch<Event> { it.id = eventLookup.eventId; it.eventTime = eventLookup.eventTime; it.shard = eventLookup.shard; it.bucketId = eventLookup.bucketId }.result { null }
                if (event == null) {
                    eventResponse.apply { id = eventLookup.eventId; eventTime = eventLookup.eventTime?.toString() }
                    status(NOT_FOUND).entity(eventResponse)
                } else {
                    event.payload = eventLookup.payload
                    if (fire) {
                        processorRegistry(event)
                    }
                    status(OK).entity(event.toResponse())
                }
            }
        } else {
            throw IllegalArgumentException("null id")
        }
    }

    class ProcessRegisterTask(private val config: ProcessorConfig) : Serializable, Callable<ProcessorConfig?> {
        override fun call() = processorRegistry.register(config)
    }

    private fun String.base64() = Base64.getEncoder().encodeToString(this.json().toByteArray())

    fun response(f: () -> Any?): Response {
        val begin = LocalDateTime.now()
        val r = try {
            f()?.run { this as? Response.ResponseBuilder ?: ok(this) }
                    ?: status(404).entity(mapOf("status" to "not found"))
        } catch (e: Exception) {
            val t = e.rootCause()!!
            l.error("error in processing request", t)
            val status = if (t is IllegalArgumentException || t is DateTimeParseException) 400 else 500
            val message = "please contact engineering team with the below error signature"
            status(status).entity(
                    mutableMapOf("message"
                            to (t.message?.let { """${t.message}${if (status == 500) " ($message)" else ""}""" }
                            ?: "Unexpected error, $message")).apply {
                        if (status == 500) {
                            this["error"] = mapOf("stack" to t.stackTraceAsString()!!,
                                    "node" to hz.hz.cluster.localMember.address.host,
                                    "start_time" to begin,
                                    "duration" to (LocalDateTime.now().toInstant(UTC).toEpochMilli() - begin.toInstant(UTC).toEpochMilli())
                            ).json().run { if (DEBUG_FLAG.get()) this else base64() }
                        }
                    }
            )
        }
        val end = LocalDateTime.now()
        r.header("Start-Time", begin).header("End-Time", end)
                .header("Duration", "${end.toInstant(UTC).toEpochMilli() - begin.toInstant(UTC).toEpochMilli()} ms")
                .header("Node", hz.hz.cluster.localMember.address.host)
        return r.build()
    }
}