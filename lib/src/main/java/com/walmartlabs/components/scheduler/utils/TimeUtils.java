package com.walmartlabs.components.scheduler.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;

import static java.time.Instant.ofEpochMilli;
import static java.time.LocalDateTime.*;
import static java.time.ZoneId.systemDefault;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;

/**
 * Created by smalik3 on 3/24/16
 */
public class TimeUtils {

    public static long toAbsolute(Long hourOffset) {
        return HOURS.addTo(of(now().getYear(), 1, 1, 0, 0), hourOffset).atZone(systemDefault()).toInstant().toEpochMilli();
    }

    public static long toOffset(Long hourInMillis) {
        return HOURS.between(of(now().getYear(), 1, 1, 0, 0), ofInstant(ofEpochMilli(hourInMillis), systemDefault()));
    }

    public static long bucketize(long instant) {
        return ofEpochMilli(instant).truncatedTo(HOURS).toEpochMilli();
    }

    public static LocalDateTime nextScan(LocalDateTime ldt, int scanInterval) {
        final Instant minutesInstant = ldt.atZone(systemDefault()).toInstant().truncatedTo(MINUTES);
        final LocalDateTime ld = LocalDateTime.ofInstant(minutesInstant, systemDefault());
        final int currentMinutes = ld.get(ChronoField.MINUTE_OF_HOUR);
        final int offset = scanInterval - currentMinutes % scanInterval;
        return ld.plusMinutes(offset);
    }
}
