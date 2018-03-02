package com.walmartlabs.opensource.bigben.api

import com.google.common.base.Throwables.getStackTraceAsString
import com.google.common.collect.ImmutableMap
import com.walmartlabs.opensource.bigben.entities.Event
import com.walmartlabs.opensource.bigben.entities.EventLookup
import com.walmartlabs.opensource.bigben.entities.EventRequest
import com.walmartlabs.opensource.bigben.entities.EventResponse
import com.walmartlabs.opensource.bigben.entities.EventStatus.REJECTED
import com.walmartlabs.opensource.bigben.entities.EventStatus.TRIGGERED
import com.walmartlabs.opensource.bigben.entities.Mode.UPSERT
import com.walmartlabs.opensource.bigben.extns.*
import com.walmartlabs.opensource.bigben.hz.Hz
import com.walmartlabs.opensource.bigben.hz.Service
import com.walmartlabs.opensource.bigben.processors.ProcessorConfig
import com.walmartlabs.opensource.bigben.processors.ProcessorRegistry
import com.walmartlabs.opensource.bigben.tasks.StatusTask
import event
import toResponse
import java.io.Serializable
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
    }

    @GET
    @Path("/cluster")
    fun clusterStats(): Map<String, String> {
        return hz.hz.getExecutorService("default").submitToAllMembers(StatusTask(service.name)).
                mapKeys { it.key.address.toString() }.mapValues { it.value.result { "Error: ${getStackTraceAsString(it)}" } }
    }

    @POST
    @Path("/schedule")
    fun schedule(events: List<EventRequest>): Response {
        return try {
            events.map { if (it.mode == UPSERT) receiver.addEvent(it) else receiver.removeEvent(it.id!!, it.tenant!!) }
                    .reduce().result { emptyList() }.run {
                filter { it.eventStatus == TRIGGERED }.map { ProcessorRegistry.instance(it.event()) }.
                        done({ l.error("error in triggering lapsed events:", it.rootCause()) }) {
                            it!!.forEach {
                                l.warn("event was triggered immediately (likely lapsed), event id: {}, tenant: {}, " +
                                        "eventTime: {}, currentTime: {}", it.xrefId, it.tenant, it.eventTime, nowUTC())
                            }
                        }
                count { it.eventStatus == REJECTED }.let {
                    when {
                        it == events.size -> status(BAD_REQUEST).entity(this).build()
                        it > 0 -> status(PARTIAL_CONTENT).entity(this).build()
                        else -> ok().entity(this).build()
                    }
                }
            }
        } catch (e: Exception) {
            val eventResponse = EventResponse()
            val rootCause = e.rootCause()
            //eventResponse.setErrors(newArrayList<Error>(Error("500", "submit", getStackTraceString(rootCause), rootCause.message, ERROR, APPLICATION)))
            status(INTERNAL_SERVER_ERROR).entity(eventResponse).build()
        }
    }

    @POST
    @Path("/register")
    fun registerProcessor(config: ProcessorConfig): Response {
        return try {
            ok().entity(hz.hz.getExecutorService("default").submitToAllMembers(ProcessRegisterTask(config)).
                    mapValues { it.value.listenable() }.values.toList().reduce().result { throw RuntimeException("") }).build()
        } catch (e: Exception) {
            l.error("error in registering processor: {}", config, e.rootCause())
            status(INTERNAL_SERVER_ERROR).entity(ImmutableMap.of("status", "FAIL", "error", getStackTraceAsString(e.rootCause()!!))).build()
        }
    }

    @GET
    @Path("/processorRegistry")
    fun registeredTenants() = ProcessorRegistry.instance.registeredConfigs()

    @GET
    @Path("/find")
    fun find(@QueryParam("id") id: String, @QueryParam("tenant") tenant: String) = EventRequest().apply { this.id = id; this.tenant = tenant }.let { find(it, false) }

    @POST
    @Path("/dryrun")
    fun dryrun(@QueryParam("id") id: String, @QueryParam("tenant") tenant: String) = EventRequest().apply { this.id = id; this.tenant = tenant }.let { find(it, true) }

    private fun find(eventRequest: EventRequest, fire: Boolean): Response {
        val eventResponse = eventRequest.toResponse()
        return if (eventRequest.id != null && eventRequest.id!!.trim().isNotEmpty()) {
            val eventLookup = fetch<EventLookup> { it.xrefId = eventRequest.id; it.tenant = eventRequest.tenant }.result { null }
            if (eventLookup == null) {
                status(NOT_FOUND).entity(eventResponse).build()
            } else {
                val event = fetch<Event> { it.id = eventLookup.eventId; it.eventTime = eventLookup.eventTime; it.shard = eventLookup.shard }.result { null }
                if (event == null) {
                    eventResponse.apply { id = eventLookup.eventId; eventTime = eventLookup.eventTime?.toString() }
                    status(NOT_FOUND).entity(eventResponse).build()
                } else {
                    event.payload = eventLookup.payload
                    if (fire) {
                        ProcessorRegistry.instance(event)
                    }
                    status(OK).entity(event.toResponse()).build()
                }
            }
        } else {
            //eventResponse.setErrors(newArrayList<Error>(Error("400.BIGBEN", "id", "id null", "id null")))
            status(BAD_REQUEST).entity(eventResponse).build()
        }
    }

    class ProcessRegisterTask(private val config: ProcessorConfig) : Serializable, Callable<ProcessorConfig?> {
        override fun call() = ProcessorRegistry.instance.register(config)
    }
}