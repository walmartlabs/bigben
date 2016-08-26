package com.walmartlabs.components.scheduler.utils;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.offsets.OffsetStore;
import com.walmartlabs.components.scheduler.entities.EventLookup;
import com.walmartlabs.components.scheduler.entities.EventLookupDO.EventLookupKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.selector;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Collections.EMPTY_MAP;
import static java.util.stream.Collectors.toList;

/**
 * Created by smalik3 on 8/15/16
 */
public class EventOffsetsStore implements OffsetStore {

    private static final Logger L = Logger.getLogger(EventOffsetsStore.class);

    @Autowired
    @Qualifier("bigbenDataManager")
    private DataManager<EventLookupKey, EventLookup> dm;

    private static final Selector<EventLookupKey, EventLookup> selector = selector(EventLookup.class, EventLookup::getPayload);

    @Override
    public ListenableFuture<?> store(String tenant, Map<TopicPartition, OffsetAndMetadata> offsets) {
        L.debug("storing offsets: " + offsets);
        return successfulAsList(offsets.entrySet().stream().map(e -> {
            final EventLookup entity = entity(EventLookup.class, new EventLookupKey("" + e.getKey().partition(), tenant + "/" + e.getKey().topic()));
            entity.setPayload("" + e.getValue().offset());
            entity.setShard(-1);
            return dm.saveAsync(entity);
        }).collect(toList()));
    }

    @Override
    public ListenableFuture<Map<TopicPartition, OffsetAndMetadata>> load(String tenant, List<TopicPartition> partitions) {
        return transform(allAsList(partitions.stream().map(p ->
                        dm.getAsync(new EventLookupKey("" + p.partition(), tenant + "/" + p.topic()), selector)).collect(toList())),
                (Function<List<EventLookup>, Map<TopicPartition, OffsetAndMetadata>>) l -> {
                    if (l == null || l.isEmpty()) return EMPTY_MAP;
                    final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    l.stream().filter(el -> el != null && el.getPayload() != null).forEach(el ->
                            map.put(new TopicPartition(el.id().getTenant().substring((tenant + "/").length()),
                                    parseInt(el.id().getXrefId())), new OffsetAndMetadata(parseLong(el.getPayload()))));
                    partitions.stream().filter(partition -> !map.containsKey(partition)).forEach(partition -> map.put(partition, new OffsetAndMetadata(-1)));
                    return map;
                });
    }
}
