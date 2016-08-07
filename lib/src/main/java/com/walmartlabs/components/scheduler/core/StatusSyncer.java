package com.walmartlabs.components.scheduler.core;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmartlabs.components.scheduler.entities.Bucket;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventDO;
import com.walmartlabs.components.scheduler.entities.Status;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.now;

/**
 * Created by smalik3 on 7/5/16
 */
class StatusSyncer {

    private static final Logger L = Logger.getLogger(StatusSyncer.class);

    private final DataManager<ZonedDateTime, Bucket> dataManager;
    private final DataManager<EventDO.EventKey, Event> dm;

    StatusSyncer(DataManager<ZonedDateTime, Bucket> dataManager, DataManager<EventDO.EventKey, Event> dm) {
        this.dataManager = dataManager;
        this.dm = dm;
    }

    ListenableFuture<Bucket> syncBucket(ZonedDateTime bucketId, Status status, boolean setProcessedAt) {
        final Bucket entity = entity(Bucket.class, bucketId);
        if (setProcessedAt)
            entity.setProcessedAt(now(UTC));
        entity.setStatus(status.name());
        L.info(format("bucket %s is done, syncing status as %s", bucketId, status));
        final ListenableFuture<Bucket> f = dataManager.saveAsync(entity);
        addCallback(f,
                new FutureCallback<Bucket>() {
                    @Override
                    public void onSuccess(Bucket result) {
                        L.info(format("bucket %s is successfully synced as %s", bucketId, status));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        L.error(format("bucket %s could not be synced as %s, after multiple retries", bucketId, status), t);
                    }
                });
        return f;
    }

    ListenableFuture<Event> syncShard(ZonedDateTime bucketId, int shard, ZonedDateTime eventTime, String eventId, Status status, String payload) {
        final Event entity = entity(Event.class, EventDO.EventKey.of(bucketId, shard, eventTime, eventId));
        entity.setStatus(status.name());
        if (payload != null)
            entity.setPayload(payload);
        L.info(format("shard %s[%d] is done, syncing status as %s, payload: %s", bucketId, shard, status, payload));
        final ListenableFuture<Event> f = dm.saveAsync(entity);
        addCallback(f,
                new FutureCallback<Event>() {
                    @Override
                    public void onSuccess(Event result) {
                        L.info(format("shard %s[%d] is successfully synced as %s", bucketId, shard, status));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        L.error(format("shard %s[%d] could not be synced as %s, after multiple retries", bucketId, shard, status), t);
                    }
                });
        return f;
    }

}
