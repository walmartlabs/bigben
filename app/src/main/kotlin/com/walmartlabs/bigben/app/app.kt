/*-
 * #%L
 * BigBen:app
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
package com.walmartlabs.bigben.app

/**
 * Created by smalik3 on 2/28/18
 */
import com.walmartlabs.bigben.BigBen
import com.walmartlabs.bigben.BigBen.module
import com.walmartlabs.bigben.api.EventReceiver
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.api.EventService.Companion.DEBUG_FLAG
import com.walmartlabs.bigben.cron.CronService
import com.walmartlabs.bigben.entities.EventRequest
import com.walmartlabs.bigben.entities.EventStatus.REJECTED
import com.walmartlabs.bigben.extns.bucket
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.utils.*
import com.walmartlabs.bigben.utils.commons.Module
import com.walmartlabs.bigben.utils.commons.ModuleRegistry
import org.slf4j.Logger
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
import javax.ws.rs.ext.Provider
import kotlin.system.exitProcess

@Provider
@PreMatching
class DebugFlagSetter : ContainerRequestFilter {
    override fun filter(ctx: ContainerRequestContext) {
        if (ctx.uriInfo.queryParameters.containsKey("debug")) DEBUG_FLAG.set(true) else DEBUG_FLAG.set(false)
    }
}

class App : Application() {

    init {
        try {
            val lifecycle =
                typeRefYaml<Map<String, String?>>(App::class.java.classLoader.getResource("bigben-lifecycle.yaml").readText())
            initPhase("pre-init", lifecycle, null)
            val l = logger<App>()
            l.info("phase:pre-init finished")
            println(
                "\n" +
                        "  ____  _       ____             \n" +
                        " |  _ \\(_)     |  _ \\            \n" +
                        " | |_) |_  __ _| |_) | ___ _ __  \n" +
                        " |  _ <| |/ _` |  _ < / _ \\ '_ \\ \n" +
                        " | |_) | | (_| | |_) |  __/ | | |\n" +
                        " |____/|_|\\__, |____/ \\___|_| |_|\n" +
                        "           __/ |                 \n" +
                        "          |___/                  \n"
            )
            BigBen.init()
            initPhase("post-init", lifecycle, l)
            l.info("Bigben => successfully started")
        } catch (e: Exception) {
            try {
                val l: Logger = logger<App>()
                l.error("Bigben:error => unknown error, system will exit", e.rootCause()!!)
            } catch (ignore: Exception) {
            }
            exitProcess(1)
        }
    }

    private fun initPhase(phase: String, lifecycle: Map<String, String?>, l: Logger?) {
        l?.info("phase:$phase started")
        lifecycle["$phase-class"]?.run { (Class.forName(this).newInstance() as Module).init(ModuleRegistry()) }
            ?: lifecycle["$phase-object"]?.run {
                (Class.forName(this).getDeclaredField("INSTANCE").apply {
                    isAccessible = true
                }.get(null) as Module).init(ModuleRegistry())
            }
        l?.info("phase:$phase finished")
    }

    override fun getSingletons() = setOf(module<EventService>(), EventGenerator, CronService)
    override fun getClasses() = setOf(DebugFlagSetter::class.java)
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
            module<EventReceiver>().addEvent(EventRequest().also {
                it.tenant = eg.tenant
                it.eventTime = t.toString()
                it.id = UUID.randomUUID().toString()
            }).transform { if (it?.eventStatus == REJECTED) throw IllegalArgumentException(it.error?.message) else it }
        }.reduce().transform {
            it!!.groupBy { ZonedDateTime.parse(it!!.eventTime).bucket() }.mapValues { it.value.size }.toSortedMap()
        }.get(30L, TimeUnit.MINUTES)
    }
}
