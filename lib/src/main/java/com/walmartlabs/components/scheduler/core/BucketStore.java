package com.walmartlabs.components.scheduler.core;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.MapStore;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManagerConfig;
import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmartlabs.components.scheduler.model.Bucket;
import com.walmartlabs.components.scheduler.utils.HackedDataManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.cache.integration.CacheLoaderException;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.fullSelector;
import static com.walmart.gmp.ingestion.platform.framework.utils.ConfigParser.parse;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Created by smalik3 on 4/1/16
 */
public class BucketStore implements MapStore<ZonedDateTime, Bucket> {

    public BucketStore(Properties properties) throws Exception {
        final String dmConfigPath = "dmConfigPath";
        checkArgument(properties != null && properties.containsKey(dmConfigPath), "null or incomplete properties: " +
                "(" + dmConfigPath + " is required)" + properties);
        final DataManagerConfig dataManagerConfig = parse(properties.getProperty(dmConfigPath), DataManagerConfig.class);
        dataManagerConfig.getInterceptorConfigs().clear();
        dataManager = new HackedDataManager<>(dataManagerConfig);
    }

    public BucketStore() {
    }

    private static final Logger L = Logger.getLogger(BucketStore.class);

    @Autowired
    private transient DataManager<ZonedDateTime, Bucket> dataManager;

    @Override
    public void store(ZonedDateTime key, Bucket value) {
        storeAll(of(key, value));
    }

    @Override
    public void storeAll(Map<ZonedDateTime, Bucket> map) {
        try {
            allAsList(map.entrySet().stream().map(e -> store0(e.getKey(), e.getValue())).collect(toList())).get(30, SECONDS);
        } catch (Exception e) {
            L.error("error in bulk store", e);
        }
    }

    private ListenableFuture<Bucket> store0(ZonedDateTime key, Bucket value) {
        if (value == null) {
            L.warn("null value for key: " + key);
            return immediateFuture(null);
        }
        L.debug(format("syncing key: %s, value: %s", key, value));
        final Bucket entity = DataManager.entity(Bucket.class, key);
        entity.setStatus(value.getStatus());
        entity.setCount(value.getCount());
        final ListenableFuture<Bucket> f = dataManager.saveAsync(entity);
        addCallback(f, new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(Bucket result) {
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
    public void delete(ZonedDateTime key) {
        L.warn("delete is not supported: " + key);
    }

    @Override
    public void deleteAll(Collection<ZonedDateTime> keys) {
        L.warn("deleteAll is not supported: " + keys);
    }

    @Override
    public Bucket load(ZonedDateTime key) {
        final Map<ZonedDateTime, Bucket> map = loadAll(singletonList(key));
        return map.isEmpty() ? null : map.entrySet().iterator().next().getValue();
    }

    @Override
    public Map<ZonedDateTime, Bucket> loadAll(Collection<ZonedDateTime> keys) {
        try {
            return allAsList(newArrayList(keys).stream().map(this::load0).
                    collect(toList())).get(30, SECONDS).stream().filter(e -> e != null).collect(
                    toMap((Function<Bucket, ZonedDateTime>) Entity::id, identity()));
        } catch (Exception e) {
            L.error("error in loading the keys: " + keys, e);
            return new HashMap<>();
        }
    }

    private ListenableFuture<Bucket> load0(ZonedDateTime key) throws CacheLoaderException {
        L.debug("loading data for key " + key);
        return transform(dataManager.getAsync(key, fullSelector(key)), //TODO: dont use full selector, no need to load the error
                (com.google.common.base.Function<Bucket, Bucket>) DataManager::raw);
    }

    @Override
    public Iterable<ZonedDateTime> loadAllKeys() {
        return null;
    }
}
