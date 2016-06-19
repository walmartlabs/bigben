package com.walmartlabs.components.scheduler.core.hz;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.core.EventProcessor;
import com.walmartlabs.components.scheduler.model.Bucket;
import com.walmartlabs.components.scheduler.model.Bucket.BucketStatus;
import com.walmartlabs.components.scheduler.model.Event;
import com.walmartlabs.components.scheduler.model.EventDO;
import com.walmartlabs.components.scheduler.model.EventDO.EventKey;
import info.archinnov.achilles.persistence.AsyncManager;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getStackTraceString;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.*;
import static java.lang.Integer.getInteger;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Created by smalik3 on 3/26/16
 */
public class EventTask implements Callable<ListenableFuture<BucketStatus>> {

    private static final Logger L = Logger.getLogger(EventTask.class);
    private transient final TaskExecutor taskExecutor = new TaskExecutor(newHashSet(Exception.class)); //TODO: narrow down the list

    private final long bucketId;
    private final Set<Integer> shards;
    private final String executionKey;

    private transient EventProcessor<Event> eventProcessor;
    private transient DataManager<EventKey, Event> eventDM;
    private transient DataManager<Long, Bucket> bucketDM;

    private static String shardRange(Set<Integer> shards) {
        return shards == null || shards.isEmpty() ? "[-]" : shards.size() == 1 ? "[" + shards.iterator().next() + "]"
                : "[" + shards.iterator().next() + "-" + newArrayList(shards).get(shards.size() - 1) + "]";
    }

    @SuppressWarnings("unchecked")
    public EventTask(long bucketId, Set<Integer> shards, DataManager<?, ?> dataManager, EventProcessor<Event> eventProcessor) {
        this.bucketId = bucketId;
        this.shards = shards;
        executionKey = format("%d/%s", bucketId, shardRange(shards));
        this.eventProcessor = eventProcessor;
        this.eventDM = (DataManager<EventKey, Event>) dataManager;
        this.bucketDM = (DataManager<Long, Bucket>) dataManager;
    }

    @Override
    public ListenableFuture<BucketStatus> call() throws Exception {
        L.debug(format("%s, processing shards", executionKey));
        final int fetchSize = PROPS.getInteger("bucketDM.events.fetch.size", 400);
        return transformAsync(successfulAsList(shards.stream().map($ ->
                        loadAndProcess($, fetchSize, getAsyncManager(), -1L)).collect(toList())),
                ll -> {
                    final List<EventKey> l = newArrayList(concat(ll)).stream().filter(e -> !PROCESSED.name().equals(e.getStatus())).
                            map(EventDO::getEventKey).collect(toList());
                    final Bucket entity = entity(Bucket.class, bucketId);
                    if (!l.isEmpty()) {
                        entity.setFailedEvents(l);
                        entity.setStatus(ERROR.name());
                    } else {
                        entity.setStatus(PROCESSED.name());
                    }
                    return transform(bucketDM.saveAsync(entity), (Function<Bucket, BucketStatus>) e -> valueOf(e.getStatus()));
                });
    }

    private ListenableFuture<List<EventDO>> loadAndProcess(int shardIndex, int fetchSize, AsyncManager am, long eventTime) {
        final String taskId = shardIndex + "[" + eventTime + "(" + fetchSize + ")]";
        L.debug(format("%s, loading and processing shardIndex %d, fetchSize %d, from eventTime: %d", executionKey, shardIndex, fetchSize, eventTime));
        @SuppressWarnings("unchecked")
        final ListenableFuture<List<EventDO>> f = async(() -> am.sliceQuery(EventDO.class).forSelect().
                withPartitionComponents(bucketId, shardIndex).fromClusterings(eventTime).withExclusiveBounds().limit(fetchSize).get(), "load-shard-slice#" + taskId);
        return transformAsync(f, l -> {
            if (l.isEmpty()) {
                L.debug(format("%s, no more events to process: shardIndex %d, fetchSize %d, from eventTime: %d", executionKey, shardIndex, fetchSize, eventTime));
                return immediateFuture(l);
            }
            return transformAsync(
                    transformAsync(
                            async(() -> successfulAsList(l.stream().filter(e ->
                                    !PROCESSED.name().equals(e.getStatus())).map(am::removeProxy).map(this::schedule).
                                    collect(toList())), "process-events#" + taskId), this::save),
                    $ -> loadAndProcess(shardIndex, fetchSize, am, l.get(l.size() - 1).id().getEventTime()));
        });
    }

    private ListenableFuture<List<EventDO>> save(List<EventDO> l) {
        L.debug(format("%s, saving event status to the db", executionKey));
        return successfulAsList(l.stream().map(e -> {
            L.debug(format("%s, saving event: %s to the DB, the status is '%s'", executionKey, e.id(), e.getStatus()));
            final Event entity = entity(Event.class, e.id());
            entity.setStatus(e.getStatus());
            if (e.getError() != null)
                entity.setError(e.getError());
            return transform(eventDM.saveAsync(entity), (Function<Event, EventDO>) $ -> (EventDO) $);
        }).collect(toList()));
    }

    private ListenableFuture<EventDO> schedule(EventDO e) {
        final long delay = e.id().getEventTime() - currentTimeMillis();
        if (delay <= 0) {
            L.debug(format("%s, event %s time has expired, processing immediately", executionKey, e.id()));
            return process(e);
        } else {
            L.debug(format("%s, scheduling event '%s' after delay %d, at %s", executionKey, e.id(), delay, new Date(e.id().getEventTime())));
            return dereference(EXECUTOR.schedule(() -> process(e), delay, MILLISECONDS));
        }
    }

    private ListenableFuture<EventDO> process(EventDO e) {
        try {
            L.debug(format("%s, processing event: %s", executionKey, e.id()));
            return transform(eventProcessor.process(e), (Function<Event, EventDO>) $ -> {
                L.debug(format("%s, processed event: %s", executionKey, e.id()));
                $.setStatus(PROCESSED.name());
                return (EventDO) $;
            });
        } catch (Throwable t) {
            L.error(format("%s, error in processing event: %s", executionKey, e.id()));
            e.setStatus(ERROR.name());
            e.setError(getStackTraceString(getRootCause(t)));
            return immediateFuture(e);
        }
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
                getInteger("bucketDM.max.retries", 3),
                getInteger("bucketDM.retry.initial.delay.ms", 1000),
                getInteger("bucketDM.retry.backoff.multiplier", 2),
                MILLISECONDS);
    }
}
