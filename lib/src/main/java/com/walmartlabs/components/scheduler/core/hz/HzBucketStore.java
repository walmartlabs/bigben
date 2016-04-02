package com.walmartlabs.components.scheduler.core.hz;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.MapStore;
import com.hazelcast.spring.context.SpringAware;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmartlabs.components.scheduler.model.EventBucketStatusEntity;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.cache.integration.CacheLoaderException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

/**
 * Created by smalik3 on 4/1/16
 */
@SpringAware
public class HzBucketStore implements MapStore<Long, EventBucketStatusEntity> {

    private static final Logger L = Logger.getLogger(HzBucketStore.class);

    @Autowired
    private transient DataManager<Long, EventBucketStatusEntity> dataManager;

    @Override
    public void store(Long key, EventBucketStatusEntity value) {
        storeAll(of(key, value));
    }

    @Override
    public void storeAll(Map<Long, EventBucketStatusEntity> map) {
        try {
            allAsList(map.entrySet().stream().map(e -> store0(e.getKey(), e.getValue())).collect(toList())).get(30, SECONDS);
        } catch (Exception e) {
            L.error("error in bulk store", e);
        }
    }

    private ListenableFuture<EventBucketStatusEntity> store0(Long key, EventBucketStatusEntity value) {
        if (value == null) {
            L.warn("null value for key: " + key);
            return immediateFuture(null);
        }
        L.debug(format("syncing key: %s, value: %s", key, value));
        final EventBucketStatusEntity entity = DataManager.entity(EventBucketStatusEntity.class, key);
        entity.setStatus(value.getStatus());
        entity.setCount(value.getCount());
        if (value.getError() != null)
            entity.setError(value.getError());
        entity.setFailedShards(value.getFailedShards());
        final ListenableFuture<EventBucketStatusEntity> f = dataManager.saveAsync(entity);
        addCallback(f, new FutureCallback<EventBucketStatusEntity>() {
            @Override
            public void onSuccess(EventBucketStatusEntity result) {
                L.debug(format("key: %s synced successfully, value: %s", key, value));
            }

            @Override
            public void onFailure(Throwable t) {
                L.error(format("could not sync key %s", getRootCause(t)));
            }
        });
        return f;
    }

    @Override
    public void delete(Long key) {
        L.warn("delete is not supported: " + key);
    }

    @Override
    public void deleteAll(Collection<Long> keys) {
        L.warn("deleteAll is not supported: " + keys);
    }

    @Override
    public EventBucketStatusEntity load(Long key) {
        final Map<Long, EventBucketStatusEntity> map = loadAll(singletonList(key));
        return map.isEmpty() ? null : map.entrySet().iterator().next().getValue();
    }

    @Override
    public Map<Long, EventBucketStatusEntity> loadAll(Collection<Long> keys) {
        try {
            return allAsList(newArrayList(keys).stream().map(this::load0).
                    collect(toList())).get(30, SECONDS).stream().filter(e -> e != null).collect(
                    toMap((Function<EventBucketStatusEntity, Long>) Entity::id, identity()));
        } catch (Exception e) {
            L.error("error in loading the keys: " + keys, e);
            return new HashMap<>();
        }
    }

    private ListenableFuture<EventBucketStatusEntity> load0(Long key) throws CacheLoaderException {
        L.debug("loading data for key " + key);
        return transform(dataManager.getAsync(key, fullSelector(key)), //TODO: dont use full selector, no need to load the error
                (com.google.common.base.Function<EventBucketStatusEntity, EventBucketStatusEntity>) DataManager::raw);
    }

    private static final AtomicLong counter = new AtomicLong();
    private static final Set<Long> ALL_KEYS = nCopies(366 * 24, 0L).stream().map($ -> counter.getAndIncrement()).collect(toSet());

    @Override
    public Iterable<Long> loadAllKeys() {
        return null;
    }
}
