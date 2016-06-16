package com.walmartlabs.components.scheduler.core.ignite;

import com.google.common.base.Function;
import com.google.common.util.concurrent.*;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.core.EventProcessor;
import com.walmartlabs.components.scheduler.model.Bucket;
import com.walmartlabs.components.scheduler.model.Event;
import com.walmartlabs.components.scheduler.model.EventDO;
import info.archinnov.achilles.persistence.AsyncManager;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import javax.cache.processor.EntryProcessor;
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
import static com.walmartlabs.components.scheduler.core.ignite.EventScheduleScanner.BUCKET_CACHE;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.ERROR;
import static com.walmartlabs.components.scheduler.model.Bucket.BucketStatus.PROCESSED;
import static java.lang.Integer.getInteger;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Created by smalik3 on 3/26/16
 */
public class ScheduleJob implements IgniteRunnable {

    private static final Logger L = Logger.getLogger(ScheduleJob.class);

    private final long bucketKey;
    private final Set<Integer> shards;
    private final long bucketOffset;
    private final String jobKey;

    @SpringResource(resourceName = "eventProcessor")
    private transient EventProcessor<Event> eventProcessor;

    @SpringResource(resourceName = "dataManager")
    private transient DataManager<EventDO.EventKey, Event> dataManager;

    @IgniteInstanceResource
    private transient Ignite ignite;

    private transient final TaskExecutor taskExecutor = new TaskExecutor(newHashSet(Exception.class)); //TODO: narrow down the list

    public ScheduleJob(long bucketKey, Set<Integer> shards, long bucketOffset) {
        this.bucketKey = bucketKey;
        this.shards = shards;
        this.bucketOffset = bucketOffset;
        jobKey = format("%d/%d/%s", bucketKey, bucketOffset, shardRange(shards));
    }

    private String shardRange(Set<Integer> shards) {
        return shards == null || shards.isEmpty() ? "[-]" : shards.size() == 1 ? "[" + shards.iterator().next() + "]"
                : "[" + shards.iterator().next() + "-" + newArrayList(shards).get(shards.size() - 1) + "]";
    }

    @Override
    public void run() {
        try {
            final CqlDAO<?, ?> cqlDAO = (CqlDAO<?, ?>) dataManager.getPrimaryDAO(V1).unwrap();
            final AsyncManager pm = cqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
            L.debug(format("%s, processing shards", jobKey));
            final ListenableFuture<List<List<Event>>> shardsFuture =
                    allAsList(shards.stream().map(s -> {
                        L.debug(format("%s, loading shard %d from the db", jobKey, s));
                        return async(() ->
                                transform(pm.sliceQuery(EventDO.class).forSelect().
                                                withPartitionComponents(bucketKey, s).get(),
                                        (Function<List<EventDO>, List<Event>>)
                                                l -> l.stream().filter(i -> !PROCESSED.name().equals(i.getState())).
                                                        map(pm::removeProxy).collect(toList())), "load-shard#" + s);
                    }).collect(toList()));
            final ListenableFuture<List<Event>> processedFuture =
                    transform(shardsFuture, (AsyncFunction<List<List<Event>>, List<Event>>) ll -> {
                        final List<Event> entities = newArrayList(concat(ll));
                        L.debug(format("%s, shards data loaded successfully, total %d events found", jobKey, entities.size()));
                        if (!entities.isEmpty())
                            L.debug(format("%s, scheduling the events", jobKey));
                        final List<ListenableFuture<Event>> processFutures =
                                entities.stream().map(e -> {
                                    final long delay = e.id().getEventTime() - currentTimeMillis();
                                    if (delay <= 0) {
                                        L.debug(format("%s, event %s time has expired, processing immediately", jobKey, e.id()));
                                        return processEvent(e);
                                    } else {
                                        L.debug(format("%s, scheduling event '%s' after delay %d, at %s", jobKey, e.id(), delay, new Date(e.id().getEventTime())));
                                        return dereference(EXECUTOR.schedule(() -> processEvent(e), delay, MILLISECONDS));
                                    }
                                }).collect(toList());
                        return allAsList(processFutures);
                    });
            final ListenableFuture<List<Event>> syncedFuture =
                    transform(processedFuture, (AsyncFunction<List<Event>, List<Event>>) l -> {
                        L.debug(format("%s, syncing event status to the db", jobKey));
                        final List<ListenableFuture<Event>> $ = l.stream().map(e -> {
                            L.debug(format("%s, syncing event: %s to the DB, the status is '%s'", jobKey, e.id(), e.getState()));
                            final Event entity = entity(Event.class, e.id());
                            entity.setState(e.getState());
                            if (e.getError() != null)
                                entity.setError(e.getError());
                            return transform(dataManager.saveAsync(entity), (Function<Event, Event>) DataManager::raw);
                        }).collect(toList());
                        return allAsList($);
                    });
            addCallback(transform(syncedFuture, (Function<List<Event>, Set<Integer>>) l -> {
                L.debug(format("%s, calculating failed shards", jobKey));
                final Set<Integer> failedShards = l.stream().filter(
                        e -> !PROCESSED.name().equals(e.getState())).map(e -> e.id().getShard()).collect(toSet());
                if (!failedShards.isEmpty())
                    L.warn(format("%s, following shards failed: %s", jobKey, failedShards));
                else
                    L.debug(format("%s, all shards were processed successfully", jobKey));
                return failedShards;
            }), new FutureCallback<Set<Integer>>() {
                @Override
                public void onSuccess(Set<Integer> failedShards) {
                    final IgniteCache<Long, Bucket> cache = ignite.cache(BUCKET_CACHE);
                    cache.invoke(bucketOffset, (EntryProcessor<Long, Bucket, Object>) (entry, arguments) -> {
                        if (entry.getValue() != null) {
                            final Bucket entity = entry.getValue();
                            entity.setJobCount(entity.getJobCount() - 1);
                            if (failedShards.isEmpty()) {
                                L.debug(format("%s, shards are done, with no errors", jobKey));
                                if (entity.getJobCount() == 0) {
                                    L.debug(format("%s, all shards done, all jobs done, marking the status as %s", jobKey, PROCESSED));
                                    entity.setStatus(PROCESSED.name());
                                }
                            } else {
                                L.debug(format("%s, following shards failed: %s", jobKey, failedShards));
                                entity.addFailedShards(failedShards);
                                if (entity.getJobCount() == 0) {
                                    L.debug(format("%s, all shards done (some with errors), all jobs done, marking the status as %s", jobKey, ERROR.name()));
                                    entity.setStatus(ERROR.name());
                                }
                            }
                            entry.setValue(entity);
                        } else {
                            L.warn("found null bucket value for key: " + entry.getKey());
                        }
                        return null;
                    });
                }

                @Override
                public void onFailure(Throwable t) {
                    reprocessAllShards(t);
                }
            });
        } catch (Throwable t) {
            reprocessAllShards(t);
        }
    }

    @NotNull
    private ListenableFuture<Event> processEvent(Event e) {
        try {
            L.debug(format("%s, processing event: %s", jobKey, e.id()));
            final ListenableFuture<Event> f = async(() -> eventProcessor.process(e), "process-event:" + e.id());
            L.debug(format("%s, processed event: %s", jobKey, e.id()));
            return transform(f, (Function<Event, Event>) $ -> {
                $.setState(PROCESSED.name());
                return $;
            });
        } catch (Throwable t) {
            L.error(format("%s, error in processing event: %s", jobKey, e.id()));
            e.setState(ERROR.name());
            e.setError(getStackTraceString(getRootCause(t)));
            return immediateFuture(e);
        }
    }

    private void reprocessAllShards(Throwable t) {
        L.error(format("%s, error in processing the job, the entire job will be re-processed", jobKey), getRootCause(t));
        final IgniteCache<Long, Bucket> cache = ignite.cache(BUCKET_CACHE);
        cache.invoke(bucketOffset, (EntryProcessor<Long, Bucket, Object>) (entry, arguments) -> {
            if (entry.getValue() != null) {
                final Bucket entity = entry.getValue();
                entity.setStatus(ERROR.name());
                entity.addFailedShards(shards);
                entry.setValue(entity);
            } else {
                L.warn(format("%s, found null bucket value for key: %d, something is broken! ", jobKey, entry.getKey()));
            }
            return null;
        });
    }

    private static final AtomicInteger ATOMIC_INTEGER = new AtomicInteger();
    private static final ListeningScheduledExecutorService EXECUTOR = listeningDecorator(
            new ScheduledThreadPoolExecutor(
                    PROPS.getInteger("evt.scheduler.worker.threads", 2 * getRuntime().availableProcessors()),
                    (r) -> new Thread(r, "evt-processor#" + ATOMIC_INTEGER.getAndIncrement()), new CallerRunsPolicy()));

    private static <V> ListenableFuture<List<V>> allAsList(List<ListenableFuture<V>> futures) {
        return Futures.allAsList(futures);
    }

    private <R> ListenableFuture<R> async(Callable<ListenableFuture<R>> task, String taskId) {
        return taskExecutor.async(task, taskId,
                getInteger("dm.max.retries", 3),
                getInteger("dm.retry.initial.delay.ms", 1000),
                getInteger("dm.retry.backoff.multiplier", 2),
                MILLISECONDS);
    }
}
