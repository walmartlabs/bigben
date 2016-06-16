package com.walmartlabs.components.scheduler.core.hz;

import com.google.common.util.concurrent.Futures;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.beust.jcommander.internal.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static java.util.stream.Collectors.toList;

/**
 * Created by smalik3 on 6/16/16
 */
public class BulkEventTask implements Callable<Map<Long, String>> {

    private final Map<Long, Set<Integer>> shards;

    public BulkEventTask(Map<Long, Set<Integer>> shards) {
        this.shards = shards;
    }

    @Override
    public Map<Long, String> call() throws Exception {
        final List<Entry<Long, Set<Integer>>> entries = newArrayList(shards.entrySet());
        final List<String> results = successfulAsList(entries.stream().map(e -> {
            try {
                return new EventTask(e.getKey(), e.getValue(), null, null).call();
            } catch (Exception ex) {
                return Futures.<String>immediateFailedFuture(ex);
            }
        }).collect(toList())).get(PROPS.getInteger("event.process.max.wait", 90),
                TimeUnit.valueOf(PROPS.getProperty("event.process.wait.units", "MINUTES")));

        final Map<Long, String> map = new HashMap<>();
        int index = 0;
        for (Entry<Long, Set<Integer>> entry : entries) {
            map.put(entry.getKey(), results.get(index++));
        }
        return map;
    }
}
