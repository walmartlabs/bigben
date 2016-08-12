package com.walmartlabs.components.tests;

import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.model.FeedItemStatusKey;
import com.walmart.gmp.ingestion.platform.framework.data.model.ItemStatusEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.setProperty;
import static java.util.UUID.randomUUID;

/**
 * Created by smalik3 on 7/28/16
 */
@ContextConfiguration(locations = {"/test-scheduler.xml"})
public class TestBatchPerf extends AbstractTestNGSpringContextTests {

    static {
        setProperty("dm.entity.packages.scan", "com");
        setProperty("com.walmart.platform.config.runOnEnv", "prod");
        setProperty("event.shard.size", "10");
        setProperty("hazelcast.slow.operation.detector.stacktrace.logging.enabled", "true");
        setProperty("com.walmart.platform.config.appName", "gmp-solr-consumer");
    }

    @Autowired
    private DataManager<FeedItemStatusKey, ItemStatusEntity> dataManager;

    @DataProvider
    public Object[][] data() {
        return new Object[][]{
                new Object[]{"logged-batch", 10000, 1000, 1000},
                new Object[]{"scatter-gather", 10000, 1000, 1000},
                new Object[]{"unlogged-batch", 10000, 1000, 1000}
                /*, new Object[]{"no-batch", 10000, 1000, 1000},*/};
    }

    @Test(dataProvider = "data", invocationCount = 10)
    public void test(String mode, int total, int batchSize, int shardSize) throws Exception {
        /*final String feedId = EntityIdFactory.createFeedId(ITEM, V2);

        // warm up:
        System.out.println("warming up");
        dataManager.save(next(EntityIdFactory.createFeedId(ITEM, V2), 0, 1).get(0));
        System.out.println("warm up over");

        System.out.println("----------------------------------------");

        switch (mode) {
            case "no-batch": {
                System.out.println("starting no batch perf test");
                final long now = currentTimeMillis();
                for (int i = 0; i < total; i++) {
                    int shard = i / shardSize;
                    final long n = currentTimeMillis();
                    final ItemStatusEntity itemStatusEntity = next(feedId, shard, 1).get(0);
                    dataManager.save(itemStatusEntity);
                    System.out.printf("item %d done, time taken: %d ms\n", (i + 1), (currentTimeMillis() - n));
                }
                final long diff = currentTimeMillis() - now;
                System.out.printf("DONE, time taken: %d ms, total items: %d, mode: %s\n", diff, total, mode);
                break;
            }
            case "logged-batch": {
                System.out.println("starting batch perf test");
                final CqlDAO<?, ?> cqlDAO = (CqlDAO<?, ?>) dataManager.getPrimaryDAO(V1).unwrap();
                final AsyncManager am = cqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
                final long now = currentTimeMillis();
                int batchIndex = 0;
                for (int i = 0; i < total; i += batchSize) {
                    int shard = i / shardSize;
                    final long n = currentTimeMillis();
                    final AsyncBatch batch = am.createLoggedBatch();
                    batch.startBatch();
                    next(feedId, shard, batchSize).forEach(entity -> batch.insertOrUpdate(raw(entity)));
                    batch.asyncEndBatch().get();
                    System.out.printf("batch %d is done, time taken: %d ms\n", ++batchIndex, (currentTimeMillis() - n));
                }
                final long diff = currentTimeMillis() - now;
                System.out.printf("DONE, time taken: %d ms, total items: %d, batch-size: %d, mode: %s\n", diff, total, batchSize, mode);
                break;
            }
            case "unlogged-batch": {
                System.out.println("starting batch perf test");
                final CqlDAO<?, ?> cqlDAO = (CqlDAO<?, ?>) dataManager.getPrimaryDAO(V1).unwrap();
                final AsyncManager am = cqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
                final long now = currentTimeMillis();
                int batchIndex = 0;
                for (int i = 0; i < total; i += batchSize) {
                    int shard = i / shardSize;
                    final long n = currentTimeMillis();
                    final AsyncBatch batch = am.createUnloggedBatch();
                    batch.startBatch();
                    next(feedId, shard, batchSize).forEach(entity -> batch.insertOrUpdate(raw(entity)));
                    batch.asyncEndBatch().get();
                    System.out.printf("batch %d is done, time taken: %d ms\n", ++batchIndex, (currentTimeMillis() - n));
                }
                final long diff = currentTimeMillis() - now;
                System.out.printf("DONE, time taken: %d ms, total items: %d, batch-size: %d, mode: %s\n", diff, total, batchSize, mode);
                break;
            }
            case "scatter-gather": {
                System.out.println("starting async no batch perf test");
                final long now = currentTimeMillis();
                int batchIndex = 0;
                for (int i = 0; i < total; i += batchSize) {
                    int shard = i / shardSize;
                    final long n = currentTimeMillis();
                    successfulAsList(next(feedId, shard, batchSize).stream().map(item -> dataManager.saveAsync(item)).collect(toList())).get();
                    System.out.printf("async no batch %d is done, time taken: %d ms\n", ++batchIndex, (currentTimeMillis() - n));
                }
                final long diff = currentTimeMillis() - now;
                System.out.printf("DONE, time taken: %d ms, total items: %d, batch-size: %d, mode: %s\n", diff, total, batchSize, mode);
                break;
            }
            default:
                throw new IllegalArgumentException("unknown mode: " + mode);
        }
        System.out.println("----------------------------------------");*/
    }

    private List<ItemStatusEntity> next(String feedId, int shard, int size) {
        final List<ItemStatusEntity> entities = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            final ItemStatusEntity entity = DataManager.entity(ItemStatusEntity.class, new FeedItemStatusKey(feedId, shard, "sku" + randomUUID().toString(), i));
            entity.setBatchId("b1");
            entity.setCrossrefItemId("cit1");
            entities.add(entity);
        }
        return entities;
    }
}
