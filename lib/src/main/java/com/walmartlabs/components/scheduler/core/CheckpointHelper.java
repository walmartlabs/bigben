package com.walmartlabs.components.scheduler.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.entities.EventLookup;
import com.walmartlabs.components.scheduler.entities.EventLookupDO.EventLookupKey;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.*;

import static com.google.common.collect.Maps.transformValues;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmart.gmp.ingestion.platform.framework.messaging.kafka.Constants.OBJECT_MAPPER;
import static java.lang.Integer.parseInt;

/**
 * Created by smalik3 on 7/5/16
 */
public class CheckpointHelper {

    private static final Logger L = Logger.getLogger(CheckpointHelper.class);

    private static final ListenableFuture<EventLookup> SUCCESS = immediateFuture(null);

    private final DataManager<EventLookupKey, EventLookup> lookupDataManager;
    private final EventLookupKey KEY = new EventLookupKey("_CHECKPOINT_", "_CHECKPOINT_");

    @SuppressWarnings("unchecked")
    CheckpointHelper(BucketManager bucketManager) {
        this.lookupDataManager = (DataManager) bucketManager.getEventDataManager();
    }

    synchronized ListenableFuture<EventLookup> saveCheckpoint(Map<ZonedDateTime, BucketSnapshot> data) {
        try {
            final Map<ZonedDateTime, BucketSnapshot> sorted = new TreeMap<>(data);
            if (L.isDebugEnabled())
                L.debug("saving checkpoint for buckets: " + sorted.keySet());
            final EventLookup entity = DataManager.entity(EventLookup.class, KEY);
            entity.setPayload(OBJECT_MAPPER.writeValueAsString(transformValues(sorted, (Function<BucketSnapshot, Map<String, Object>>) bs ->
                    ImmutableMap.of("b", bs.id().toString(), "c", bs.count(), "a", from(bs.awaiting()), "p", from(bs.processing()))).values()));
            return lookupDataManager.saveAsync(entity);
        } catch (Exception e) {
            L.error("error in saving the checkpoint: buckets: " + data, e);
            return SUCCESS; // we do not want to suppress further executions of this timer
        }
    }

    ListenableFuture<Map<ZonedDateTime, BucketSnapshot>> loadCheckpoint() {
        final ListenableFuture<EventLookup> f = lookupDataManager.getAsync(KEY, fullSelector(KEY));
        return catching(transform(f, (Function<EventLookup, Map<ZonedDateTime, BucketSnapshot>>) result -> {
            try {
                if (result == null) {
                    L.info("no checkpoint to load");
                    return new HashMap<>();
                } else if (result.getPayload() != null && result.getPayload().trim().length() > 0) {
                    final List<Map<String, Object>> l = OBJECT_MAPPER.readValue(result.getPayload(), new TypeReference<List<Map<String, Object>>>() {
                    });
                    final Map<ZonedDateTime, BucketSnapshot> data = new HashMap<>();
                    l.forEach(d -> {
                        final ZonedDateTime id = ZonedDateTime.parse(d.get("b").toString());
                        final BitSet awaiting = to(d.get("a").toString());
                        final BitSet processing = to(d.get("p").toString());
                        processing.stream().forEach(awaiting::set);
                        data.put(id, new BucketSnapshot(id, (Integer) d.get("c"), awaiting, new BitSet()));
                    });
                    return data;
                } else return new HashMap<>();
            } catch (IOException e) {
                L.error("error in loading the checkpoint, ignoring", e);
                return new HashMap<>();
            }
        }), Exception.class, ex -> {
            L.error("error in loading checkpoint, ignoring", ex);
            return new HashMap<>();
        });
    }

    private String from(BitSet bitSet) {
        return Joiner.on(",").join(bitSet.stream().boxed().toArray());
    }

    private BitSet to(String bits) {
        final BitSet bs = new BitSet();
        if (bits == null || bits.trim().length() == 0)
            return bs;
        Splitter.on(",").split(bits).forEach(s -> bs.set(parseInt(s)));
        return bs;
    }

}
