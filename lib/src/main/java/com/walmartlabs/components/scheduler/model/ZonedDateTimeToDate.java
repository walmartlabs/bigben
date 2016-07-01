package com.walmartlabs.components.scheduler.model;

import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.time.ZonedDateTime;
import java.util.Date;

import static com.walmartlabs.components.scheduler.utils.TimeUtils.fromDate;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.toDate;

/**
 * Created by smalik3 on 6/30/16
 */
public class ZonedDateTimeToDate implements Codec<ZonedDateTime, Date> {
    @Override
    public Class<ZonedDateTime> sourceType() {
        return ZonedDateTime.class;
    }

    @Override
    public Class<Date> targetType() {
        return Date.class;
    }

    @Override
    public Date encode(ZonedDateTime fromJava) throws AchillesTranscodingException {
        return toDate(fromJava);
    }

    @Override
    public ZonedDateTime decode(Date fromCassandra) throws AchillesTranscodingException {
        return fromDate(fromCassandra);
    }
}
