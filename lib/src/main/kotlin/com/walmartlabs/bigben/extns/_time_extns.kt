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
package com.walmartlabs.bigben.extns

import com.walmartlabs.bigben.utils.utils.Props
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
        throw NullPointerException("null time"), Props.int("events.schedule.scan.interval.minutes")))
