package com.walmartlabs.components.scheduler.entities.codecs;

import com.walmartlabs.components.scheduler.entities.EventLookupDO.EventLookupKey;
import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.exception.AchillesTranscodingException;

/**
 * Created by smalik3 on 7/6/16
 */
public class EventLookupKeyToString implements Codec<EventLookupKey, String> {
    @Override
    public Class<EventLookupKey> sourceType() {
        return EventLookupKey.class;
    }

    @Override
    public Class<String> targetType() {
        return String.class;
    }

    @Override
    public String encode(EventLookupKey fromJava) throws AchillesTranscodingException {
        return fromJava.getXrefId();
    }

    @Override
    public EventLookupKey decode(String fromCassandra) throws AchillesTranscodingException {
        return new EventLookupKey(fromCassandra);
    }
}
