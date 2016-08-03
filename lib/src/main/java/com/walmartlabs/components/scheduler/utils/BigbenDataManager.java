package com.walmartlabs.components.scheduler.utils;

import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManagerConfig;
import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.entities.Bucket;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventLookup;
import com.walmartlabs.components.scheduler.processors.ProcessorConfig;
import com.walmartlabs.components.scheduler.processors.ProcessorRegistry;
import info.archinnov.achilles.options.Options;
import info.archinnov.achilles.persistence.AsyncManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;
import static info.archinnov.achilles.options.OptionsBuilder.withTtl;

/**
 * Created by smalik3 on 6/30/16
 */
public class BigbenDataManager<K, T extends Entity<K>> extends DataManager<K, T> {

    @Autowired
    private ProcessorRegistry processorRegistry;

    public BigbenDataManager(String configPath) throws Exception {
        super(configPath);
    }

    public BigbenDataManager(DataManagerConfig dataManagerConfig) throws Exception {
        super(dataManagerConfig);
    }

    public BigbenDataManager(String configPath, Map<String, ?> objectLookup) throws Exception {
        super(configPath, objectLookup);
    }

    public BigbenDataManager(DataManagerConfig dataManagerConfig, Map<String, ?> objectLookup) throws Exception {
        super(dataManagerConfig, objectLookup);
    }

    public BigbenDataManager(DataManager<K, T> dataManager) throws Exception {
        super(dataManager);
    }

    @Override
    public ListenableFuture<T> performSave(T entity, Options options) {
        return super.performSave(entity, newOptions(options, entity));
    }

    @Override
    public ListenableFuture<T> performInsert(T entity, Options options) {
        return super.performInsert(entity, newOptions(options, entity));
    }

    @Override
    public ListenableFuture<T> performGet(Class<T> implClass, K key, Selector<K, T> selector, Options options) {
        @SuppressWarnings("unchecked")
        final CqlDAO<K, T> cqlDAO = (CqlDAO<K, T>) getPrimaryDAO(V1).unwrap();
        final AsyncManager am = cqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
        return am.find(implClass, key);
    }

    private Options newOptions(Options options, T entity) {
        if (true)
            return options;
        if (entity instanceof Event) {
            final Event event = (Event) entity;
            return withTtl(retention(event.getTenant())).
                    withConsistency(options.getConsistencyLevel().get()).
                    withTimestamp(options.getTimestamp().get());
        } else if (entity instanceof EventLookup) {
            final EventLookup eventLookup = (EventLookup) entity;
            return withTtl(retention(eventLookup.id().getTenant())).
                    withConsistency(options.getConsistencyLevel().get()).
                    withTimestamp(options.getTimestamp().get());
        } else if (entity instanceof Bucket) {
            return withTtl(1). //TODO change 1:
                    withConsistency(options.getConsistencyLevel().get()).
                    withTimestamp(options.getTimestamp().get());
        }
        throw new Error("unreachable");
    }

    private int retention(String tenant) {
        final ProcessorConfig processorConfig = processorRegistry.registeredConfigs().get(tenant);
        return (int) processorConfig.getProperties().get("retention");
    }
}
