package com.walmartlabs.components.scheduler.core;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.model.Event;
import com.walmartlabs.components.scheduler.model.EventDO;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;
import info.archinnov.achilles.persistence.AsyncManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getStackTraceString;
import static com.walmart.services.common.util.JsonUtil.convertToObject;
import static com.walmart.services.common.util.JsonUtil.convertToString;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.ERROR;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.PROCESSED;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.utc;
import static java.lang.Integer.getInteger;
import static java.lang.Long.MIN_VALUE;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Created by smalik3 on 3/26/16
 */
public class ShardTask implements Callable<ListenableFuture<ShardStatus>> {

    private static final Logger L = Logger.getLogger(ShardTask.class);
    private transient final TaskExecutor taskExecutor = new TaskExecutor(newHashSet(Exception.class)); //TODO: narrow down the list

    private final long bucketId;
    private final int shard;
    private final String executionKey;

    private transient EventProcessor<Event> eventProcessor;
    private transient DataManager<EventKey, Event> eventDM;

    @SuppressWarnings("unchecked")
    ShardTask(long bucketId, int shard, DataManager<?, ?> dm, EventProcessor<Event> ep) {
        this.bucketId = bucketId;
        this.shard = shard;
        this.eventProcessor = ep;
        this.eventDM = (DataManager<EventKey, Event>) dm;
        executionKey = format("%s[%d]", utc(bucketId).toString(), shard);
    }

    @Override
    public ListenableFuture<ShardStatus> call() throws Exception {
        L.debug(format("%s, processing shard", executionKey));
        final int fetchSize = PROPS.getInteger("events.fetch.size", 400);
        final EventKey errorKey = EventKey.of(bucketId, shard, MIN_VALUE, "");
        return catching(transformAsync(eventDM.getAsync(errorKey, fullSelector(errorKey)), evt -> {
            if (evt == null) {
                L.debug(format("%s, processing shard from beginning", executionKey));
                return transformAsync(loadAndProcess(fetchSize, getAsyncManager(), -1, ""), this::calculateShardStatus);
            } else {
                L.debug(format("%s, processing just the errored out events", executionKey));
                return transformAsync(allAsList(convertToObject(evt.getError(), new TypeReference<List<EventDO>>() {
                }).stream().map(this::schedule).collect(toList())), this::calculateShardStatus);
            }
        }), Exception.class, ex -> {
            L.error(format("%s, error in processing shard", executionKey));
            return new ShardStatus(bucketId, shard, ERROR);
        });
    }

    private ListenableFuture<ShardStatus> calculateShardStatus(List<EventDO> l) throws IOException {
        final List<EventDO> events = l.stream().filter(e -> e != null && !PROCESSED.name().equals(e.getStatus())).map(DataManager::raw).collect(toList());
        if (!events.isEmpty()) {
            L.info(format("%s, errors in processing shard: %s", executionKey, events));
            final Event entity = entity(Event.class, EventKey.of(bucketId, shard, MIN_VALUE, ""));
            entity.setStatus(ERROR.name());
            entity.setError(convertToString(events));
            return transform(eventDM.saveAsync(entity), (Function<Event, ShardStatus>) $ -> new ShardStatus(bucketId, shard, ERROR));
        } else {
            L.info(format("%s, shard processed successfully", executionKey));
            return immediateFuture(new ShardStatus(bucketId, shard, PROCESSED));
        }
    }

    private ListenableFuture<List<EventDO>> loadAndProcess(int fetchSize, AsyncManager am, long eventTime, String eventId) {
        final String taskId = shard + "[" + eventTime + "(" + fetchSize + ")]";
        L.debug(format("%s, loading and processing shard %d, fetchSize %d, from eventTime: %d", executionKey, shard, fetchSize, eventTime));
        @SuppressWarnings("unchecked")
        final ListenableFuture<List<EventDO>> f = async(() -> am.sliceQuery(EventDO.class).forSelect().
                withPartitionComponents(bucketId, shard).fromClusterings(eventTime, eventId).withExclusiveBounds().limit(fetchSize).get(), "load-shard-slice#" + taskId);
        return transformAsync(f, l -> {
            if (l.isEmpty()) {
                L.debug(format("%s, no more events to process: shard %d, fetchSize %d, (eventTime, eventId): (%d,%s)", executionKey, shard, fetchSize, eventTime, eventId));
                return immediateFuture(l);
            }
            final ListenableFuture<List<EventDO>> schedule = async(() -> successfulAsList(l.stream().filter(e ->
                    e != null && !PROCESSED.name().equals(e.getStatus())).map(am::removeProxy).map(this::schedule).
                    collect(toList())), "process-events#" + taskId);
            return transformAsync(schedule, $ -> {
                if (l.size() == fetchSize)
                    return loadAndProcess(fetchSize, am, l.get(l.size() - 1).id().getEventTime(), l.get(l.size() - 1).id().getEventId());
                else {
                    L.debug(format("%s, no more events to process: shard %d, fetchSize %d, (eventTime, eventId): (%d,%s)", executionKey, shard, fetchSize, eventTime, eventId));
                    return immediateFuture(l);
                }
            });
        });
    }

    private ListenableFuture<EventDO> schedule(EventDO e) {
        final long delay = e.id().getEventTime() - currentTimeMillis();
        if (delay <= 0) {
            L.debug(format("%s, event %s time has expired, processing immediately", executionKey, e.id()));
            return transformAsync(process(e), $ -> save(e));
        } else {
            L.debug(format("%s, scheduling event '%s' after delay %d, at %s", executionKey, e.id(), delay, new Date(e.id().getEventTime())));
            final ListenableFuture<EventDO> process = dereference(EXECUTOR.schedule(() -> process(e), delay, MILLISECONDS));
            return transformAsync(process, $ -> save(e));
        }
    }

    private ListenableFuture<EventDO> process(EventDO e) {
        try {
            L.debug(format("%s, processing event: %s", executionKey, e.id()));
            return transform(eventProcessor.process(e), (Function<Event, EventDO>) $ -> {
                L.debug(format("%s, processed event: %s", executionKey, e.id()));
                e.setStatus(PROCESSED.name());
                return e;
            });
        } catch (Throwable t) {
            L.error(format("%s, error in processing event: %s", executionKey, e.id()));
            e.setStatus(ERROR.name());
            e.setError(getStackTraceString(getRootCause(t)));
            return immediateFuture(e);
        }
    }

    private ListenableFuture<EventDO> save(EventDO e) {
        L.debug(format("%s, saving event: %s to the DB, the status is '%s'", executionKey, e.id(), e.getStatus()));
        final Event entity = entity(Event.class, e.id());
        entity.setStatus(e.getStatus());
        if (e.getError() != null)
            entity.setError(e.getError());
        return transform(eventDM.saveAsync(entity), (Function<Event, EventDO>) $ -> (EventDO) $);
    }

    private AsyncManager getAsyncManager() {
        final CqlDAO<?, ?> cqlDAO = (CqlDAO<?, ?>) eventDM.getPrimaryDAO(V1).unwrap();
        return cqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
    }

    private static final AtomicInteger ATOMIC_INTEGER = new AtomicInteger();
    private static final ListeningScheduledExecutorService EXECUTOR = listeningDecorator(
            new ScheduledThreadPoolExecutor(
                    PROPS.getInteger("evt.scheduler.worker.threads", 2 * getRuntime().availableProcessors()),
                    (r) -> new Thread(r, "evt-processor#" + ATOMIC_INTEGER.getAndIncrement()), new CallerRunsPolicy()));

    private <R> ListenableFuture<R> async(Callable<ListenableFuture<R>> task, String taskId) {
        return taskExecutor.async(task, taskId,
                getInteger("dm.max.retries", 3),
                getInteger("dm.retry.initial.delay.ms", 1000),
                getInteger("dm.retry.backoff.multiplier", 2),
                MILLISECONDS);
    }
}
