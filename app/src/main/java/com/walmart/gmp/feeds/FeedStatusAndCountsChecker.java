package com.walmart.gmp.feeds;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.core.FeedType;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmart.gmp.ingestion.platform.framework.data.model.*;
import com.walmart.gmp.ingestion.platform.framework.data.model.impl.v2.index.*;
import com.walmart.gmp.ingestion.platform.framework.feed.FeedData.FeedStatus;
import com.walmart.marketplace.messages.v1_feedstatus.Feedstatus;
import com.walmart.partnerapi.error.GatewayError;
import com.walmart.partnerapi.model.v2.ItemStatus;
import com.walmart.services.common.util.JsonUtil;
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.processors.EventProcessor;
import info.archinnov.achilles.persistence.AsyncManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;

import java.util.*;
import java.util.Map.Entry;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.retryableExceptions;
import static com.walmart.gmp.ingestion.platform.framework.data.core.EntityVersion.V2;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.selector;
import static com.walmart.gmp.ingestion.platform.framework.feed.FeedData.FeedStatus.*;
import static com.walmart.gmp.ingestion.platform.framework.feed.FeedData.FeedStatus.INPROGRESS;
import static com.walmart.gmp.ingestion.platform.framework.utils.PerfUtils.measure;
import static com.walmart.partnerapi.model.v2.ItemStatus.*;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getStackTraceString;
import static com.walmart.services.common.util.JsonUtil.convertToString;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Created by smalik3 on 8/24/16
 * Modified by cshah for adding Solr sync and inventory feed handling.
 */
public class FeedStatusAndCountsChecker implements EventProcessor<Event>, InitializingBean {

    private static final Logger L = Logger.getLogger(FeedStatusAndCountsChecker.class);

    AsyncManager itemAM;
    AsyncManager itemInventoryAM;

    private int fetchSize;

    public static final String FEED = "feed";
    public static final String ITEM = "item";
    public static final String ITEM_INDEX = "item_index";
    public static final String INVENTORY = "inventory";
    public static final String INVENTORY_INDEX = "inventory_index";

    public static final Selector<String, FeedStatusEntity> feedSelector =
            selector(FeedStatusEntity.class, e -> {
                e.getEntity_count();
                e.getSystemErrorCount();
                e.getTimeoutErrorCount();
                e.getSuccessCount();
                e.getDataErrorCount();
                e.getFeed_status();
            });

    public final Map<String, DataManager<?, ?>> map = new HashMap<>();

    public FeedStatusAndCountsChecker(Map<String, Object> properties) throws Exception {
        createDM(properties.get("feedDMCCMPath"), FEED);
        createDM(properties.get("itemDMCCMPath"), ITEM);
        createDM(properties.get("itemIndexDMCCMPath"), ITEM_INDEX);
        createDM(properties.get("inventoryItemIndexDMCCMPath"), INVENTORY_INDEX);
        createDM(properties.get("inventoryItemDMCCMPath"), INVENTORY);
        afterPropertiesSet();
    }

    public void createDM(Object ccmPath, Object dmName) throws Exception {
        final DataManager<Object, Entity<Object>> dm = new DataManager<>(ccmPath.toString());
        map.put(dmName.toString(), dm);
    }

    @SuppressWarnings("unchecked")
    public <K, T extends Entity<K>> DataManager<K, T> dm(String key) {
        return (DataManager<K, T>) map.get(key);
    }

    @Override
    public ListenableFuture<Event> process(Event event) {
        L.info(format("feedId: %s, processing event for status and counts: ", event.getXrefId()));
        String payload = event.getPayload();
        Feedstatus feedstatus =  getEventFeedStatus(payload);
        FeedType feedType = getFeedType(feedstatus == null ? FeedType.ITEM.name() : feedstatus.getFeedType());
        final DataManager<String, FeedStatusEntity> feedDM = dm(FEED);
        final DataManager<FeedItemStatusKey, ItemStatusEntity> itemDM = dm(ITEM);
        final DataManager<V2ItemStatusIndexKey, V2ItemStatusIndexEntity> itemIndexDM = dm(ITEM_INDEX);

        try {
            final String feedId = event.getXrefId();
            return measure(feedId, L, () -> catching(transformAsync(feedDM.getAsync(feedId, feedSelector), fes -> {
                final FeedStatusEntity entity = entity(FeedStatusEntity.class, feedId);
                entity.setModified_by("BigBen");
                final int itemCount = fes.getEntity_count() == null ? 0 : parseInt(fes.getEntity_count());
                final String feedStatus = fes.getFeed_status();
                if (itemCount == 0) {
                    L.debug(format("feedId: %s has item count as 0", feedId));
                    if (!ERROR.name().equals(feedStatus) || !PROCESSED.name().equals(feedStatus)) {
                        L.warn(format("feedId: %s was not marked %s or %s, marking it as %s", feedId, ERROR, PROCESSED, ERROR));
                        entity.setFeed_status(ERROR.name());
                        entity.setError_code(convertToString(createGatewayError()));
                        entity.setModified_dtm(new Date());
                        return transform(feedDM.saveAsync(entity), (Function<FeedStatusEntity, Event>) $ -> event);
                    } else {
                        L.info(format("feedId: %s was already marked as %s, nothing to do", feedId, feedStatus));
                        return immediateFuture(event);
                    }
                }
                switch (FeedStatus.valueOf(feedStatus == null ? INPROGRESS.name() : feedStatus)) {
                    case PROCESSED:
                        L.info(format("feedId: %s is already marked PROCESSED, nothing to do", feedId));
                        return immediateFuture(event);
                    case RECEIVED:
                        L.warn(format("feedId: %s, is still in received status: %s, marking all items timed out", feedId, feedStatus));
                        entity.setSuccessCount(0);
                        entity.setSystemErrorCount(0);
                        entity.setDataErrorCount(0);
                        entity.setTimeoutErrorCount(itemCount);
                        entity.setError_code(convertToString(createGatewayError()));
                        entity.setFeed_status(ERROR.name());
                        entity.setModified_dtm(new Date());
                        return transform(feedDM.saveAsync(entity), (Function<FeedStatusEntity, Event>) $ -> event);
                    case INPROGRESS:
                        L.warn(format("feedId: %s, is still in progress status: %s, initiating the time out procedure", feedId, feedStatus));
                        final Map<ItemStatus, Integer> countsMap = new HashMap<>();
                        return transformAsync(calculateCounts(feedId, itemCount, PROPS.getInteger("feeds.shard.size", 1000), countsMap, feedType), $ -> {
                            entity.setSuccessCount(countsMap.get(SUCCESS));
                            entity.setSystemErrorCount(countsMap.get(SYSTEM_ERROR));
                            entity.setDataErrorCount(countsMap.get(DATA_ERROR));
                            int nonTimeOuts = 0;
                            int timeOuts = 0;
                            for (Entry<ItemStatus, Integer> e : countsMap.entrySet()) {
                                if (e.getKey() != TIMEOUT_ERROR)
                                    nonTimeOuts += e.getValue();
                                else timeOuts += e.getValue();
                            }
                            if (nonTimeOuts + timeOuts == itemCount) {
                                L.warn(format("feedId: %s, was processed, but counts were not synced up, syncing them, counts are: %s", feedId, countsMap));
                                entity.setTimeoutErrorCount(countsMap.get(TIMEOUT_ERROR));
                            } else {
                                L.warn(format("feedId: %s has timed out, marking the final status and counts: %s", feedId, countsMap));
                                entity.setTimeoutErrorCount(itemCount - nonTimeOuts);
                            }
                            entity.setFeed_status(PROCESSED.name());
                            entity.setModified_dtm(new Date());
                            return transform(feedDM.saveAsync(entity), (Function<FeedStatusEntity, Event>) $$ -> event);
                        });
                    case ERROR:
                        L.info(format("feedId: %s is already marked ERROR, nothing to do", feedId));
                        return immediateFuture(event);
                    default:
                        throw new IllegalArgumentException(format("unknown feed status: %s for feedId: %s", feedStatus, feedId));
                }
            }), Exception.class, ex -> {
                final Throwable cause = getRootCause(ex);
                if (retryableExceptions.contains(cause.getClass())) {
                    L.warn(format("feedId: %s, event failed with a retryable error" + event, event.getXrefId()), cause);
                    event.setError(getStackTraceString(cause));
                } else
                    L.error(format("feedId: %s, event failed with a non-retryable error, will not be tried again: " + event, event.getXrefId()), cause);
                return event;
            }), "feed-status-count-checker");
        } catch (Exception e) {
            final Throwable cause = getRootCause(e);
            if (retryableExceptions.contains(cause.getClass())) {
                L.warn(format("feedId: %s, event failed with a retryable error" + event, event.getXrefId()), cause);
                event.setError(getStackTraceString(cause));
            } else
                L.error(format("feedId: %s, event failed with a non-retryable error, will not be tried again: " + event, event.getXrefId()), cause);
            return immediateFuture(event);
        }
    }


    private FeedType getFeedType(String feedTypeStr) {

        if(StringUtils.equalsIgnoreCase(feedTypeStr, FeedType.INVENTORY.name())) {
            return FeedType.INVENTORY;
        }
        else if(StringUtils.equalsIgnoreCase(feedTypeStr, FeedType.PRICE.name())) {
            return FeedType.PRICE;
        }
        else {
            return FeedType.ITEM;
        }
    }
    private GatewayError createGatewayError() {
        GatewayError gatewayError = new GatewayError();
        gatewayError.setCode("ERR_PDI_0001");
        gatewayError.setType("SYSTEM_ERROR");
        gatewayError.setInfo("Feed SLA violated");
        gatewayError.setDescription("Feed SLA violated. Please reach out to operations team to (possibly) re-ingest this feed");
        gatewayError.setComponent("BigBen");
        return gatewayError;
    }

    private static final ListenableFuture<List<StrippedItemEntity>> DONE = immediateFuture(null);

    private ListenableFuture<List<StrippedItemEntity>> calculateCounts(String feedId, int itemCount, int shardSize, Map<ItemStatus, Integer> counts, FeedType feedType) {
        int numShards = itemCount % shardSize == 0 ? itemCount / shardSize : 1 + itemCount / shardSize;
        final List<Integer> shards = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            shards.add(i);
        }
        L.info(format("feedId: %s, calculating counts, spanning over shards: %s", feedId, shards));
        return transform(allAsList(shards.stream().map(shard -> calculateCounts0(feedId, shard, -1, counts, feedType)).filter(out -> out != null).collect(toList())),
                (Function<List<List<StrippedItemEntity>>, List<StrippedItemEntity>>) ll -> {
                    if (ll == null) {
                        return null;
                    } else {
                        return newArrayList(concat(ll.stream().filter(strippedItemDOs -> strippedItemDOs != null).collect(toList())));
                    }
                });
    }

    private final TaskExecutor taskExecutor = new TaskExecutor(retryableExceptions);

    private ListenableFuture<List<StrippedItemEntity>> calculateCounts0(String feedId, int shard, int itemIndex, Map<ItemStatus, Integer> counts, FeedType feedType) {
        ListenableFuture<List<StrippedItemDO>> f = immediateFuture(null);
       if(feedType != null && feedType.equals(FeedType.INVENTORY)) {
           f = taskExecutor.async(() -> itemInventoryAM.sliceQuery(StrippedItemDO.class).forSelect().
                   withPartitionComponents(feedId, shard).fromClusterings(itemIndex).withExclusiveBounds().
                   limit(fetchSize).get(), "fetch-feed-items-inventory:" + feedId + "(" + itemIndex + ", Inf)", 3, 1000, 2, MILLISECONDS);
       }
       else {
            f = taskExecutor.async(() -> itemAM.sliceQuery(StrippedItemDO.class).forSelect().
                            withPartitionComponents(feedId, shard).fromClusterings(itemIndex).withExclusiveBounds().
                            limit(fetchSize).get(), "fetch-feed-items:" + feedId + "(" + itemIndex + ", Inf)", 3, 1000, 2, MILLISECONDS);
        }
       return processFeedCounts(feedId, shard, counts, feedType, f);
    }

    private ListenableFuture<List<StrippedItemEntity>> processFeedCounts(final String feedId, int shard, Map<ItemStatus, Integer> counts, FeedType feedType, ListenableFuture<List<StrippedItemDO>> f) {
        return transformAsync(f, l -> {
            final List<StrippedItemEntity> inprogress = l.stream().filter(e -> "INPROGRESS".equals(e.getItem_processing_status())).collect(toList());
            final List<StrippedItemEntity> doneItems = l.stream().filter(e -> !"INPROGRESS".equals(e.getItem_processing_status())).collect(toList());
            if (!inprogress.isEmpty()) {
                L.warn(format("feedId: %s, following items are still in progress, marking them timed out: " + inprogress, feedId));
            }
            if (doneItems != null) {
                doneItems.forEach(e -> {
                    final ItemStatus status = ItemStatus.valueOf(e.getItem_processing_status());
                    if (!counts.containsKey(status))
                        counts.put(status, 0);
                    counts.put(status, counts.get(status) + 1);
                });
                addCallback(updateIndexDB(doneItems,feedType), new FutureCallback<List<StrippedItemEntity>>() {
                    @Override
                    public void onSuccess(List<StrippedItemEntity> result) {
                        L.info(format("feedId: %s, item status updated successfully in solr: " + doneItems, feedId));
                    }
                    @Override
                    public void onFailure(Throwable t) {
                        L.error(format("feedId: %s, failed to update item status in solr: " + doneItems, feedId), getRootCause(t));
                    }
                });
            }
            if (l.isEmpty() || l.size() < fetchSize) {
                return inprogress.isEmpty() ? DONE : tof(inprogress, feedId, feedType);
            } else
                return inprogress.isEmpty() ? calculateCounts0(feedId, shard, l.get(l.size() - 1).getId().getSkuIndex(), counts, feedType) :
                        transform(allAsList(calculateCounts0(feedId, shard, l.get(l.size() - 1).getId().getSkuIndex(), counts, feedType), tof(inprogress, feedId, feedType)),
                                new Function<List<List<StrippedItemEntity>>, List<StrippedItemEntity>>() {
                                    @Override
                                    public List<StrippedItemEntity> apply(List<List<StrippedItemEntity>> ll) {
                                        return ll.get(0);
                                    }
                                });
        });
    }


    private ListenableFuture<List<StrippedItemEntity>> tof(List<StrippedItemEntity> inprogress, String feedId, FeedType feedType) {
        if(feedType != null && feedType.equals(FeedType.INVENTORY)) {
            return collectInventoryItemEntity(inprogress,feedId);
         }
        return collectItemEntity(inprogress, feedId);
    }


    private ListenableFuture<List<StrippedItemEntity>> collectItemEntity(List<StrippedItemEntity> inprogress, String feedId) {
            return transform(allAsList(inprogress.stream().map(k -> {
            final ItemStatusEntity entity = entity(ItemStatusEntity.class,
                    FeedItemStatusKey.of(k.id().getFeedId(), k.id().getShard(), k.id().getSku(), k.id().getSkuIndex()));
            if (L.isDebugEnabled())
                L.debug(format("feedId: %s, marking item as timed out: %s", feedId, inprogress));
            entity.setItem_processing_status(TIMEOUT_ERROR.name());
            entity.setModified_dtm(new Date());
            return updateBothDBs(entity, feedId);
        }).collect(toList())), (Function<List<ItemStatusEntity>, List<StrippedItemEntity>>) $ -> inprogress);
    }

    private ListenableFuture<List<StrippedItemEntity>> collectInventoryItemEntity(List<StrippedItemEntity> inprogress, String feedId) {
        return transform(allAsList(inprogress.stream().map(k -> {
            final InventoryFeedItemStatusEntity entity = entity(InventoryFeedItemStatusEntity.class,
                    InventoryFeedItemStatusKey.of(k.id().getFeedId(), k.id().getShard(), k.id().getSku(), k.id().getSkuIndex()));
            if (L.isDebugEnabled())
                L.debug(format("feedId: %s, marking inventory item as timed out: %s", feedId, inprogress));
            entity.setItemStatus(TIMEOUT_ERROR.name());
            entity.setModifiedDtm(new Date());
            return updateBothDBsInventory(entity,feedId);
        }).collect(toList())), (Function<List<InventoryFeedItemStatusEntity>, List<StrippedItemEntity>>) $ -> inprogress);
    }


    private ListenableFuture<InventoryFeedItemStatusEntity> updateBothDBsInventory(InventoryFeedItemStatusEntity entity, String feedId) {

        final DataManager<InventoryFeedItemStatusKey, InventoryFeedItemStatusEntity> itemDM = dm(INVENTORY);
        final DataManager<V2InventoryStatusIndexKey, V2InventoryItemStatusIndexEntity> itemIndexDM = dm(INVENTORY_INDEX);
        final V2InventoryItemStatusIndexEntity v2entity =  entity(V2InventoryItemStatusIndexEntity.class,
                V2InventoryStatusIndexKey.from(InventoryFeedItemStatusKey.of(entity.id().getFeedId(), entity.id().getShard(),
                        entity.id().getSku(), Integer.valueOf(entity.id().getItemIndex()))));
        if (L.isDebugEnabled())
            L.debug(format("feedId: %s, syncing inventory item status in in both DBs: %s", feedId, entity.id()));
        v2entity.setItemStatus(entity.getItemStatus());
        v2entity.setModifiedDtm(new Date());
        addCallback(itemIndexDM.saveAsync(v2entity), new FutureCallback<V2InventoryItemStatusIndexEntity>() {
            @Override
            public void onSuccess(V2InventoryItemStatusIndexEntity result) {
                if (L.isDebugEnabled())
                    L.debug(format("feedId: %s, successfully updated status in index DB: %s", feedId, result.id()));
            }

            @Override
            public void onFailure(Throwable t) {
                L.error(format("feedId: %s, failed to update status in index DB", feedId), getRootCause(t));
            }
        });
        return itemDM.saveAsync(entity);
    }

    private ListenableFuture<ItemStatusEntity> updateBothDBs(ItemStatusEntity entity, String feedId) {
            final DataManager<FeedItemStatusKey, ItemStatusEntity> itemDM = dm(ITEM);
            final DataManager<V2ItemStatusIndexKey, V2ItemStatusIndexEntity> itemIndexDM = dm(ITEM_INDEX);
        final V2ItemStatusIndexEntity v2entity = entity(V2ItemStatusIndexEntity.class,
                V2ItemStatusIndexKey.from(FeedItemStatusKey.of(entity.id().getFeedId(), entity.id().getShard(),
                        entity.id().getSku(), entity.id().getSkuIndex())));
        if (L.isDebugEnabled())
            L.debug(format("feedId: %s, syncing item status in in both DBs: %s", feedId, entity.id()));
        v2entity.setItem_processing_status(entity.getItem_processing_status());
        v2entity.setModified_dtm(new Date());
        addCallback(itemIndexDM.saveAsync(v2entity), new FutureCallback<V2ItemStatusIndexEntity>() {
            @Override
            public void onSuccess(V2ItemStatusIndexEntity result) {
                if (L.isDebugEnabled())
                    L.debug(format("feedId: %s, successfully updated status in index DB: %s", feedId, result.id()));
            }

            @Override
            public void onFailure(Throwable t) {
                L.error(format("feedId: %s, failed to update status in index DB", feedId), getRootCause(t));
            }
        });
        return itemDM.saveAsync(entity);
    }

    public ListenableFuture<List<StrippedItemEntity>> updateIndexDB(List<StrippedItemEntity> done, FeedType feedType) {
        if(feedType.equals(FeedType.INVENTORY)) {
            return updateInventoryIndexDB(done);
        }
        return updateIndexDB(done);
    }

    private ListenableFuture<List<StrippedItemEntity>> updateInventoryIndexDB(List<StrippedItemEntity> done) {
        return transform(allAsList(done.stream().map(k -> {
            final V2InventoryItemStatusIndexEntity v2entity = entity(V2InventoryItemStatusIndexEntity.class,
                    V2InventoryStatusIndexKey.from(InventoryFeedItemStatusKey.of(k.id().getFeedId(), k.id().getShard(),
                            k.id().getSku(), k.id().getSkuIndex())));
            L.debug(format("feedId: %s, syncing inventory item status in solr: %s", k.id().getFeedId(), k));
            v2entity.setItemStatus(k.getItem_processing_status());
            v2entity.setModifiedDtm(new Date());
            final DataManager<V2InventoryStatusIndexKey, V2InventoryItemStatusIndexEntity> itemIndexDM = dm(INVENTORY_INDEX);
            return itemIndexDM.saveAsync(v2entity);
        }).collect(toList())), (Function<List<V2InventoryItemStatusIndexEntity>, List<StrippedItemEntity>>) $ -> done);
    }

    private ListenableFuture<List<StrippedItemEntity>> updateIndexDB(List<StrippedItemEntity> done) {
        return transform(allAsList(done.stream().map(k -> {
            final V2ItemStatusIndexEntity v2entity = entity(V2ItemStatusIndexEntity.class,
                    V2ItemStatusIndexKey.from(FeedItemStatusKey.of(k.id().getFeedId(), k.id().getShard(),
                            k.id().getSku(), k.id().getSkuIndex())));
            L.debug(format("feedId: %s, syncing item status in solr: %s", k.id().getFeedId(), k));
            v2entity.setItem_processing_status(k.getItem_processing_status());
            v2entity.setModified_dtm(new Date());
            final DataManager<V2ItemStatusIndexKey, V2ItemStatusIndexEntity> itemIndexDM = dm(ITEM_INDEX);
            return itemIndexDM.saveAsync(v2entity);
        }).collect(toList())), (Function<List<V2ItemStatusIndexEntity>, List<StrippedItemEntity>>) $ -> done);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        final DataManager<FeedItemStatusKey, ItemStatusEntity> itemDM = dm(ITEM);
        final DataManager<InventoryFeedItemStatusKey, InventoryFeedItemStatusEntity> itemInventoryDM = dm(INVENTORY);
        final CqlDAO<?, ?> feedCqlDAO = (CqlDAO<?, ?>) itemDM.getPrimaryDAO(V2).unwrap();
        final CqlDAO<?, ?> feedCqlDAO0 = (CqlDAO<?, ?>) itemInventoryDM.getPrimaryDAO(V2).unwrap();
        itemInventoryAM = feedCqlDAO0.cqlDriverConfig().getAsyncPersistenceManager();
        itemAM = feedCqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
        fetchSize = PROPS.getInteger("feed.items.fetch.size", 400);
    }

    private Feedstatus getEventFeedStatus(String payload) {
        try {
            Feedstatus feedstatus = JsonUtil.convertToObject(payload, Feedstatus.class);
            return feedstatus;
        } catch (Exception e) {
            L.error("Json conversion failed " + payload,  e);
        }
        return null;
    }
}
