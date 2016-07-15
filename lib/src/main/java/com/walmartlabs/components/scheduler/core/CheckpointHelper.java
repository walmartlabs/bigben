package com.walmartlabs.components.scheduler.core;

import com.google.common.collect.Table;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventDO;
import com.walmartlabs.components.scheduler.entities.Status;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Splitter.on;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmartlabs.components.scheduler.entities.Status.PROCESSING;
import static com.walmartlabs.components.scheduler.entities.Status.UN_PROCESSED;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.epoch;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.nowUTC;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.tuple.Pair.of;

/**
 * Created by smalik3 on 7/5/16
 */
public class CheckpointHelper {

    private static final Logger L = Logger.getLogger(CheckpointHelper.class);

    private static final ListenableFuture<Event> SUCCESS = immediateFuture(null);

    private final BucketManager bucketManager;

    public CheckpointHelper(BucketManager bucketManager) {
        this.bucketManager = bucketManager;
    }

    synchronized ListenableFuture<Event> saveCheckpoint(Table<ZonedDateTime, Integer, Status> shards) {
        try {
            L.info("saving checkpoint for shards: " + shards);
            return bucketManager.getStatusSyncer().syncShard(epoch(), -1, epoch(), "", PROCESSING, shards.cellSet().stream().sorted((o1, o2) -> {
                final int i = o1.getRowKey().compareTo(o2.getRowKey());
                return i != 0 ? i : o1.getColumnKey().compareTo(o2.getColumnKey());
            }).map(c -> format("%s/%s/%s", c.getRowKey(), c.getColumnKey(), c.getValue())).collect(joining(",")));
        } catch (Exception e) {
            L.error("error in saving the checkpoint: shards: " + shards, e);
            return SUCCESS; // we do not want to suppress further executions of this timer
        }
    }

    ListenableFuture<?> loadCheckpoint(Table<ZonedDateTime, Integer, Status> shards) {
        final EventDO.EventKey key = EventDO.EventKey.of(epoch(), -1, epoch(), "");
        final ListenableFuture<Event> f = bucketManager.getEventDataManager().getAsync(key, fullSelector(key));
        addCallback(f, new FutureCallback<Event>() {
            @Override
            public void onSuccess(Event result) {
                if (result == null)
                    L.info("no checkpoint to load");
                else if (result.getPayload() != null && result.getPayload().trim().length() > 0) {
                    on(",").split(result.getPayload()).forEach(s -> {
                        final List<String> split = newArrayList(on("/").split(s));
                        final ZonedDateTime bucketId = ZonedDateTime.parse(split.get(0));
                        final int shard = Integer.parseInt(split.get(1));
                        final Status status = Status.valueOf(split.get(2));
                        final Collection<Pair<ZonedDateTime, Integer>> pairs = new ArrayList<>();
                        if (MINUTES.between(bucketId, nowUTC()) > bucketManager.getMaxProcessingTime() && status == PROCESSING) {
                            L.warn(format("processing time for shard %s[%d] has expired, marking it unprocessed", bucketId, shard));
                            shards.put(bucketId, shard, UN_PROCESSED);
                        } else if (status == PROCESSING) {
                            shards.put(bucketId, shard, status);
                            pairs.add(of(bucketId, shard));
                        } else
                            shards.put(bucketId, shard, status);
                        if (!pairs.isEmpty()) {
                            bucketManager.startShardsTimer(pairs);
                        }
                    });
                    L.info("loaded the checkpoint: " + shards);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                L.error("could not load the previously saved bucket status checkpoint", t);
            }
        });
        return f;
    }

}
