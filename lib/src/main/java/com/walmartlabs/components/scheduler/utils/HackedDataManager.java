package com.walmartlabs.components.scheduler.utils;

import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManagerConfig;
import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmart.services.nosql.data.CqlDAO;
import info.archinnov.achilles.persistence.AsyncManager;
import info.archinnov.achilles.options.Options;

import java.util.Map;

import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V1;

/**
 * Created by smalik3 on 6/30/16
 */
public class HackedDataManager<K, T extends Entity<K>> extends DataManager<K, T> {

    public HackedDataManager(String configPath) throws Exception {
        super(configPath);
    }

    public HackedDataManager(DataManagerConfig dataManagerConfig) throws Exception {
        super(dataManagerConfig);
    }

    public HackedDataManager(String configPath, Map<String, ?> objectLookup) throws Exception {
        super(configPath, objectLookup);
    }

    public HackedDataManager(DataManagerConfig dataManagerConfig, Map<String, ?> objectLookup) throws Exception {
        super(dataManagerConfig, objectLookup);
    }

    public HackedDataManager(DataManager<K, T> dataManager) throws Exception {
        super(dataManager);
    }

    @Override
    public ListenableFuture<T> performGet(Class<T> implClass, K key, Selector<K, T> selector, Options options) {
        @SuppressWarnings("unchecked")
        final CqlDAO<K, T> cqlDAO = (CqlDAO<K, T>) getPrimaryDAO(V1).unwrap();
        final AsyncManager am = cqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
        return am.find(implClass, key);
    }
}
