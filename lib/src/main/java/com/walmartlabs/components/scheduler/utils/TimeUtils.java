package com.walmartlabs.components.scheduler.utils;

import java.time.Instant;
import java.time.ZonedDateTime;

import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.ofInstant;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;

/**
 * Created by smalik3 on 3/24/16
 */
public class TimeUtils {

    public static long bucketize(long instant, int bucketWidth) {
        final Instant epoch = ofEpochMilli(instant);
        final long hours = epoch.truncatedTo(HOURS).toEpochMilli();
        final long mins = epoch.truncatedTo(MINUTES).toEpochMilli();
        final long delta = (mins - hours) / (60 * 1000);
        final long boundary = (delta / bucketWidth) * bucketWidth;
        return hours + boundary * 60 * 1000;
    }

    public static ZonedDateTime nextScan(ZonedDateTime zdt, int scanInterval) {
        final ZonedDateTime minZdt = zdt.toInstant().truncatedTo(MINUTES).atZone(UTC);
        final int currentMinutes = minZdt.get(MINUTE_OF_HOUR);
        final int offset = scanInterval - currentMinutes % scanInterval;
        return zdt.plusMinutes(offset).withSecond(0).with(MILLI_OF_SECOND, 0).withNano(0);
    }

    public static ZonedDateTime utc(long millis) {
        return ofInstant(ofEpochMilli(millis), UTC);
    }
}
