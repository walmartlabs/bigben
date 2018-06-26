package com.walmartlabs.bigben.app

/**
 * Created by smalik3 on 2/28/18
 */
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider
import com.walmartlabs.bigben.BigBen
import com.walmartlabs.bigben.api.EventService.Companion.DEBUG_FLAG
import com.walmartlabs.bigben.entities.EventRequest
import com.walmartlabs.bigben.entities.EventStatus.REJECTED
import com.walmartlabs.bigben.extns.bucket
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.utils.*
import java.time.Duration
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import javax.ws.rs.Consumes
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.container.ContainerRequestFilter
import javax.ws.rs.container.PreMatching
import javax.ws.rs.core.Application
import javax.ws.rs.core.MediaType.APPLICATION_JSON
import javax.ws.rs.core.Response
import javax.ws.rs.ext.ExceptionMapper
import javax.ws.rs.ext.Provider

@Provider
@Consumes("*/*")
@Produces("*/*")
class JsonProvider : JacksonJsonProvider(om)

@Provider
@Consumes("*/*")
@Produces(APPLICATION_JSON)
class ExceptionMapper : ExceptionMapper<Throwable> {
    override fun toResponse(t: Throwable): Response {
        val r = BigBen.eventService.response { throw t }
        return Response.status(r.status)
                .entity(r.entity.json()).apply { r.headers.forEach { t, u -> u.forEach { header(t, it) } } }
                .header("Content-Type", "application/json")
                .build()
    }
}

@Provider
@PreMatching
class DebugFlagSetter : ContainerRequestFilter {
    override fun filter(ctx: ContainerRequestContext) {
        if (ctx.uriInfo.queryParameters.containsKey("debug")) DEBUG_FLAG.set(true) else DEBUG_FLAG.set(false)
    }
}

class App : Application() {

    private val l = logger<App>()

    init {
        System.setProperty("props", "file://bigben.yaml")
        l.info("initializing BigBen")
        println("\n" +
                "  ____  _       ____             \n" +
                " |  _ \\(_)     |  _ \\            \n" +
                " | |_) |_  __ _| |_) | ___ _ __  \n" +
                " |  _ <| |/ _` |  _ < / _ \\ '_ \\ \n" +
                " | |_) | | (_| | |_) |  __/ | | |\n" +
                " |____/|_|\\__, |____/ \\___|_| |_|\n" +
                "           __/ |                 \n" +
                "          |___/                  \n")
        l.info("$BigBen initialization complete")
    }

    override fun getSingletons() = setOf(BigBen.eventService, EventGenerator)
    override fun getClasses() = setOf(JsonProvider::class.java, com.walmartlabs.bigben.app.ExceptionMapper::class.java, DebugFlagSetter::class.java)
}

@Path("/generation")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
object EventGenerator {

    data class EventGeneration(val offset: String, val period: String, val numEvents: Int, val tenant: String)

    private val l = logger<EventGenerator>()

    @POST
    @Path("/random")
    fun generateEvents(eg: EventGeneration): Map<ZonedDateTime, Int> {
        val random = ThreadLocalRandom.current()
        val t1 = nowUTC().bucket() + Duration.parse(eg.offset)
        val t2 = t1 + Duration.parse(eg.period)
        val delta = ChronoUnit.MILLIS.between(t1, t2)
        l.info("generating ${eg.numEvents} random events between $t1 and $t2")
        return (1..eg.numEvents).map {
            val t = t1.plus(random.nextLong(delta), ChronoUnit.MILLIS)
            BigBen.eventReceiver.addEvent(EventRequest().also {
                it.tenant = eg.tenant
                it.eventTime = t.toString()
                it.id = UUID.randomUUID().toString()
            }).transform { if (it?.eventStatus == REJECTED) throw IllegalArgumentException(it.error?.message) else it }
        }.reduce().transform {
            it!!.groupBy { ZonedDateTime.parse(it!!.eventTime).bucket() }.mapValues { it.value.size }.toSortedMap()
        }.get(30L, TimeUnit.MINUTES)
    }
}