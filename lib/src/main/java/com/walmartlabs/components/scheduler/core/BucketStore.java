package com.walmartlabs.components.scheduler.core;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.MapStore;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmart.gmp.ingestion.platform.framework.data.core.QueryHelper;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.model.EventBucketStatusDO;
import com.walmartlabs.components.scheduler.model.EventBucketStatusEntity;
import info.archinnov.achilles.persistence.PersistenceManager;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.SpringResource;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.raw;
import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static com.walmart.gmp.ingestion.platform.framework.data.core.QueryHelper.entityMeta;
import static com.walmart.gmp.ingestion.platform.framework.data.core.QueryHelper.prepare;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Created by smalik3 on 3/21/16
 */
public class BucketStore implements CacheStore<Long, EventBucketStatusEntity>, Serializable {

    private static final Logger L = Logger.getLogger(BucketStore.class);

    @SpringResource(resourceName = "dataManager")
    private transient DataManager<Long, EventBucketStatusEntity> dataManager;

    @Override
    public void loadCache(IgniteBiInClosure<Long, EventBucketStatusEntity> clo, @Nullable Object... args) throws CacheLoaderException {
        L.info("bootstrapping the cache");
        try {
            prepare(EventBucketStatusDO.class);
            final CqlDAO cqlDAO = (CqlDAO) dataManager.getPrimaryDAO(V1).unwrap();
            final PersistenceManager pm = cqlDAO.cqlDriverConfig().getPersistenceManager();

            final Select select = select().from(cqlDAO.cqlDriverConfig().getCqlConfig().getKeySpace(),
                    entityMeta(EventBucketStatusDO.class).getTableName()).limit(366 * 24); //TODO: load in the steps of 1000
            final String query = select.getQueryString();

            L.info("executing query " + query);
            final PreparedStatement statement = pm.getNativeSession().prepare(query);
            final AtomicInteger counter = new AtomicInteger();
            addCallback(taskExecutor.async(() -> pm.getNativeSession().executeAsync(statement.bind()), "cache-bootstrap", 3, 1, 2, SECONDS),
                    new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(ResultSet result) {
                            final QueryHelper<Long, EventBucketStatusDO> qh = new QueryHelper<>(EventBucketStatusDO.class, result, fullSelector(0L));
                            result.forEach(row -> {
                                final EventBucketStatusEntity entity = qh.apply(row);
                                L.debug(format("loaded key '%s'", entity.id()));
                                clo.apply(entity.id(), raw(entity));
                                counter.incrementAndGet();
                            });
                            L.info("cache bootstrapped with " + counter.get() + " keys");
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            L.warn("Could not bootstrap the cache", t);
                        }
                    });
        } catch (Exception e) {
            L.warn("Could not bootstrap the cache", e);
        }
    }

    private transient final TaskExecutor taskExecutor = new TaskExecutor(newHashSet(NoHostAvailableException.class, QueryExecutionException.class));

    @Override
    public EventBucketStatusEntity load(Long key) throws CacheLoaderException {
        final Map<Long, EventBucketStatusEntity> map = loadAll(singletonList(key));
        return map.isEmpty() ? null : map.entrySet().iterator().next().getValue();
    }

    private ListenableFuture<EventBucketStatusEntity> load0(Long key) throws CacheLoaderException {
        L.debug("loading data for key " + key);
        return transform(dataManager.getAsync(key, fullSelector(key)), //TODO: dont use full selector, no need to load the error
                (com.google.common.base.Function<EventBucketStatusEntity, EventBucketStatusEntity>) DataManager::raw);
    }

    @Override
    public Map<Long, EventBucketStatusEntity> loadAll(Iterable<? extends Long> iterable) throws CacheLoaderException {
        try {
            return allAsList(newArrayList(iterable).stream().map(this::load0).
                    collect(toList())).get(30, SECONDS).stream().filter(e -> e != null).collect(
                    toMap((Function<EventBucketStatusEntity, Long>) Entity::id, identity()));
        } catch (Exception e) {
            throw new CacheLoaderException(e);
        }
    }

    @Override
    public void write(Entry<? extends Long, ? extends EventBucketStatusEntity> entry) throws CacheWriterException {
        writeAll(singletonList(entry));
    }

    private ListenableFuture<EventBucketStatusEntity> write0(Entry<? extends Long, ? extends EventBucketStatusEntity> entry) {
        if (entry.getValue() == null) {
            L.warn("null value for key: " + entry.getKey());
            return immediateFuture(entry.getValue());
        }
        L.debug(format("syncing key: %s, value: %s", entry.getKey(), entry.getValue()));
        final EventBucketStatusEntity entity = DataManager.entity(EventBucketStatusEntity.class, entry.getKey());
        entity.setStatus(entry.getValue().getStatus());
        entity.setCount(entry.getValue().getCount());
        if (entry.getValue().getError() != null)
            entity.setError(entry.getValue().getError());
        entity.setFailedShards(entry.getValue().getFailedShards());
        final ListenableFuture<EventBucketStatusEntity> f = dataManager.saveAsync(entity);
        addCallback(f, new FutureCallback<EventBucketStatusEntity>() {
            @Override
            public void onSuccess(EventBucketStatusEntity result) {
                L.debug(format("key: %s synced successfully, value: %s", entry.getKey(), entry.getValue()));
            }

            @Override
            public void onFailure(Throwable t) {
                L.error(format("could not sync key %s", getRootCause(t)));
            }
        });
        return f;
    }

    @Override
    public void writeAll(Collection<Entry<? extends Long, ? extends EventBucketStatusEntity>> collection) throws CacheWriterException {
        try {
            allAsList(collection.stream().map(this::write0).collect(toList())).get(30, SECONDS);
        } catch (Exception e) {
            throw new CacheWriterException(e);
        }
    }

    @Override
    public void sessionEnd(boolean commit) throws CacheWriterException {
    }

    @Override
    public void delete(Object o) throws CacheWriterException {
        L.warn("delete is not supported, key to be deleted: " + o);
    }

    @Override
    public void deleteAll(Collection<?> collection) throws CacheWriterException {
        collection.forEach(this::delete);
    }
}
