package com.walmartlabs.opensource.bigben.extns

import com.walmartlabs.opensource.bigben.utils.Props
import java.time.Instant.EPOCH
import java.time.Instant.ofEpochMilli
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.time.ZonedDateTime.now
import java.time.ZonedDateTime.ofInstant
import java.time.temporal.ChronoField.MILLI_OF_SECOND
import java.time.temporal.ChronoField.MINUTE_OF_HOUR
import java.time.temporal.ChronoUnit.HOURS
import java.time.temporal.ChronoUnit.MINUTES
import java.util.*

/**
 * Created by smalik3 on 2/21/18
 */
fun bucketize(instant: Long, bucketWidth: Int): Long {
    val epoch = ofEpochMilli(instant)
    val hours = epoch.truncatedTo(HOURS).toEpochMilli()
    val mins = epoch.truncatedTo(MINUTES).toEpochMilli()
    val delta = (mins - hours) / (60 * 1000)
    val boundary = delta / bucketWidth * bucketWidth
    return hours + boundary * 60 * 1000
}

fun nextScan(zdt: ZonedDateTime, scanInterval: Int): ZonedDateTime {
    val minZdt = zdt.toInstant().truncatedTo(MINUTES).atZone(UTC)
    val currentMinutes = minZdt.get(MINUTE_OF_HOUR)
    val offset = scanInterval - currentMinutes % scanInterval
    return zdt.plusMinutes(offset.toLong()).withSecond(0).with(MILLI_OF_SECOND, 0).withNano(0)
}

fun utc(millis: Long): ZonedDateTime {
    return ofInstant(ofEpochMilli(millis), UTC)
}

private val epochZdt = ofInstant(EPOCH, UTC)

fun epoch(): ZonedDateTime {
    return epochZdt
}

fun ZonedDateTime.toDate(): Date {
    return Date(toInstant().toEpochMilli())
}

fun Date.toZdt(): ZonedDateTime {
    return utc(time)
}

fun nowUTC(): ZonedDateTime {
    return now(UTC)
}

fun ZonedDateTime?.bucket() = utc(bucketize(this?.toInstant()?.toEpochMilli() ?:
        throw IllegalArgumentException("null time"), Props.int("event.schedule.scan.interval.minutes", 1)))