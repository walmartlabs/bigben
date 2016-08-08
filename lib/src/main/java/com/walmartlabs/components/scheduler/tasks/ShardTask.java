package com.walmartlabs.components.scheduler.tasks;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventDO.EventKey;
import com.walmartlabs.components.scheduler.processors.EventProcessor;
import info.archinnov.achilles.persistence.AsyncManager;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.AbstractDAO.implClass;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getStackTraceString;
import static com.walmartlabs.components.scheduler.entities.Status.ERROR;
import static com.walmartlabs.components.scheduler.entities.Status.PROCESSED;
import static com.walmartlabs.components.scheduler.utils.TimeUtils.epoch;
import static java.lang.Integer.getInteger;
import static java.lang.Integer.max;
import static java.lang.Math.min;
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

    private final ZonedDateTime bucketId;
    private final int shard;
    private final String executionKey;

    private transient EventProcessor<Event> eventProcessor;
    private transient DataManager<EventKey, Event> eventDM;
    private final int fetchSizeHint;

    @SuppressWarnings("unchecked")
    ShardTask(ZonedDateTime bucketId, int shard, DataManager<?, ?> dm, EventProcessor<Event> ep, int fetchSizeHint) {
        this.bucketId = bucketId;
        this.shard = shard;
        this.eventProcessor = ep;
        this.eventDM = (DataManager<EventKey, Event>) dm;
        this.fetchSizeHint = fetchSizeHint;
        executionKey = format("%s[%d]", bucketId, shard);
    }

    @Override
    public ListenableFuture<ShardStatus> call() throws Exception {
        final int fetchSize = max(10, min(fetchSizeHint, 400));
        L.info(format("%s, processing shard with fetch size: %d", executionKey, fetchSize));
        return catching(transform(loadAndProcess(fetchSize, getAsyncManager(), epoch(), ""), (Function<List<Event>, ShardStatus>) l -> {
            final List<Event> events = l.stream().filter(e -> e == null || !PROCESSED.name().equals(e.getStatus())).map(DataManager::raw).collect(toList());
            if (!events.isEmpty()) {
                L.info(format("%s, errors in processing shard: %s", executionKey, events));
                return new ShardStatus(bucketId, shard, ERROR);
            } else {
                L.info(format("%s, shard processed successfully", executionKey));
                return new ShardStatus(bucketId, shard, PROCESSED);
            }
        }), Exception.class, ex -> {
            L.error(format("%s, error in processing shard", executionKey), ex);
            return new ShardStatus(bucketId, shard, ERROR);
        });
    }

    private ListenableFuture<List<Event>> loadAndProcess(int fetchSize, AsyncManager am, ZonedDateTime eventTime, String eventId) {
        final String taskId = shard + "[" + eventTime + "(" + fetchSize + ")]";
        L.debug(format("%s, loading and processing shard %d, fetchSize %d, from eventTime: %s", executionKey, shard, fetchSize, eventTime));
        final Class<Event> implClass = implClass(V1, EventKey.class);
        @SuppressWarnings("unchecked")
        final ListenableFuture<List<Event>> f = async(() -> am.sliceQuery(implClass).forSelect().
                withPartitionComponents(bucketId, shard).fromClusterings(eventTime, eventId).withExclusiveBounds().limit(fetchSize).get(), "load-shard-slice#" + taskId);
        return transformAsync(f, l -> {
            if (l.isEmpty()) {
                L.debug(format("%s, no more events to process: shard %d, fetchSize %d, (eventTime, eventId): (%s,%s)", executionKey, shard, fetchSize, eventTime, eventId));
                return immediateFuture(l);
            }
            final ListenableFuture<List<Event>> schedule = async(() -> successfulAsList(l.stream().filter(e ->
                    e != null && !PROCESSED.name().equals(e.getStatus())).map(am::removeProxy).map(this::schedule).
                    collect(toList())), "process-events#" + taskId);
            return transformAsync(schedule, $ -> {
                if (l.size() == fetchSize)
                    return loadAndProcess(fetchSize, am, l.get(l.size() - 1).id().getEventTime(), l.get(l.size() - 1).id().getEventId());
                else {
                    L.debug(format("%s, no more events to process: shard %d, fetchSize %d, (eventTime, eventId): (%s,%s)", executionKey, shard, fetchSize, eventTime, eventId));
                    return immediateFuture(l);
                }
            });
        });
    }

    private ListenableFuture<Event> schedule(Event e) {
        final long delay = e.id().getEventTime().toInstant().toEpochMilli() - currentTimeMillis();
        if (delay <= 0) {
            L.debug(format("%s, event %s time has expired, processing immediately", executionKey, e.id()));
            return transformAsync(process(e), $ -> save(e));
        } else {
            L.debug(format("%s, scheduling event '%s' after delay %d, at %s", executionKey, e.id(), delay, e.id().getEventTime()));
            final ListenableFuture<Event> process = dereference(EXECUTOR.schedule(() -> process(e), delay, MILLISECONDS));
            return transformAsync(process, $ -> save(e));
        }
    }

    private ListenableFuture<Event> process(Event e) {
        try {
            L.debug(format("%s, processing event: %s", executionKey, e.id()));
            return catching(transform(eventProcessor.process(e), (Function<Event, Event>) $ -> {
                L.debug(format("%s, processed event: %s", executionKey, e.id()));
                return e;
            }), Exception.class, ex -> {
                L.error(format("%s, error in processing event, marking it %s", executionKey, ERROR));
                if (e.getStatus() == null) {
                    e.setStatus(ERROR.name());
                    e.setError(getStackTraceString(getRootCause(ex)));
                }
                return e;
            });
        } catch (Throwable t) {
            L.error(format("%s, error in processing event: %s", executionKey, e.id()));
            e.setStatus(ERROR.name());
            e.setError(getStackTraceString(getRootCause(t)));
            return immediateFuture(e);
        }
    }

    private ListenableFuture<Event> save(Event e) {
        L.debug(format("%s, saving event: %s to the DB, the status is '%s'", executionKey, e.id(), e.getStatus()));
        final Event entity = entity(Event.class, e.id());
        entity.setStatus(e.getStatus());
        entity.setProcessedAt(e.getProcessedAt());
        if (e.getError() != null)
            entity.setError(e.getError());
        return transform(eventDM.saveAsync(entity), (Function<Event, Event>) $ -> e);
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
