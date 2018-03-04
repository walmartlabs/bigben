package com.walmartlabs.opensource.bigben.app

import com.walmartlabs.opensource.bigben.api.EventReceiver
import com.walmartlabs.opensource.bigben.entities.EventRequest
import com.walmartlabs.opensource.bigben.entities.EventStatus
import com.walmartlabs.opensource.bigben.extns.bucket
import com.walmartlabs.opensource.bigben.extns.reduce
import com.walmartlabs.opensource.bigben.extns.transform
import com.walmartlabs.opensource.bigben.processors.ProcessorRegistry
import event
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit.MINUTES

/**
 * Created by smalik3 on 3/3/18
 */
data class EventGeneration(val startTime: String, val period: Long, val numEvents: Int, val tenant: String)

fun generateEvents(eg: EventGeneration, er: EventReceiver): Map<ZonedDateTime, Int> {
    val random = ThreadLocalRandom.current()
    val t1 = ZonedDateTime.parse(eg.startTime)
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
    }.get(30L, MINUTES)
}