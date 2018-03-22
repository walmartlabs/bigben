package com.walmartlabs.opensource.bigben.app

/**
 * Created by smalik3 on 2/28/18
 */
import com.walmartlabs.opensource.bigben.api.EventReceiver
import com.walmartlabs.opensource.bigben.api.EventService
import com.walmartlabs.opensource.bigben.core.ScheduleScanner
import com.walmartlabs.opensource.bigben.entities.EventRequest
import com.walmartlabs.opensource.bigben.entities.EventStatus
import com.walmartlabs.opensource.bigben.extns.bucket
import com.walmartlabs.opensource.bigben.extns.nowUTC
import com.walmartlabs.opensource.bigben.processors.ProcessorRegistry
import com.walmartlabs.opensource.bigben.providers.domain.cassandra.ClusterConfig
import com.walmartlabs.opensource.core.fromJson
import com.walmartlabs.opensource.core.hz.ClusterSingleton
import com.walmartlabs.opensource.core.hz.Hz
import com.walmartlabs.opensource.core.json
import com.walmartlabs.opensource.core.reduce
import com.walmartlabs.opensource.core.transform
import event
import spark.Spark.*
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit


fun main(args: Array<String>) {
    init(args)
}

fun init(args: Array<String>) {
    val cc = ClusterConfig()
    cc.contactPoints = "127.0.0.1"
    System.setProperty("bigben.cassandra.config", cc.json())
    System.setProperty("skip.tenant.validation", "yes")
    System.setProperty("bigben.hz.config", mapOf<String, Any>(
            "hz" to mapOf("autoIncrement" to true),
            "map" to mapOf("store" to mapOf<String, Any>("writeDelay" to 10))).json())

    val hz = Hz()
    val service = ScheduleScanner(hz)
    ClusterSingleton(service, hz)
    val eventReceiver = EventReceiver(hz)
    val es = EventService(hz, service, eventReceiver)

    val applicationJson = "application/json"

    println("\n" +
            "  ____  _       ____             \n" +
            " |  _ \\(_)     |  _ \\            \n" +
            " | |_) |_  __ _| |_) | ___ _ __  \n" +
            " |  _ <| |/ _` |  _ < / _ \\ '_ \\ \n" +
            " | |_) | | (_| | |_) |  __/ | | |\n" +
            " |____/|_|\\__, |____/ \\___|_| |_|\n" +
            "           __/ |                 \n" +
            "          |___/                  \n")

    port(args[0].toInt())
    path("/events") {
        before("/*") { _, resp -> resp.type(applicationJson) }
        get("/ping", applicationJson) { _, resp ->
            resp.status(200)
            mapOf("status" to "OK").json()
        }
        get("/cluster", applicationJson) { _, _ ->
            es.clusterStats().json()
        }
        post("/generate", applicationJson) { r, _ ->
            generateEvents(EventGeneration::class.java.fromJson(r.body()), eventReceiver).json()
        }
    }
}

data class EventGeneration(val offset: Long, val period: Long, val numEvents: Int, val tenant: String)

fun generateEvents(eg: EventGeneration, er: EventReceiver): Map<ZonedDateTime, Int> {
    val random = ThreadLocalRandom.current()
    val t1 = nowUTC().bucket().plusMinutes(eg.offset)
    val t2 = t1.plusMinutes(eg.period)
    val delta = ChronoUnit.MILLIS.between(t1, t2)
    return (1..eg.numEvents).map {
        val t = t1.plus(random.nextLong(delta), ChronoUnit.MILLIS)
        er.addEvent(EventRequest().also {
            it.tenant = eg.tenant
            it.eventTime = t.toString()
            it.id = UUID.randomUUID().toString()
        }).transform {
            if (it!!.eventStatus != EventStatus.ACCEPTED) {
                ProcessorRegistry.instance.invoke(it.event())
            }
            it
        }
    }.reduce().transform {
        it!!.groupBy { ZonedDateTime.parse(it!!.eventTime).bucket() }.mapValues { it.value.size }
    }.get(30L, TimeUnit.MINUTES)
}