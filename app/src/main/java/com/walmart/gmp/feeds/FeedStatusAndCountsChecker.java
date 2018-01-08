package com.walmart.gmp.feeds;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.walmart.gmp.ingestion.platform.framework.core.FeedType;
import com.walmart.gmp.ingestion.platform.framework.data.core.*;
import com.walmart.gmp.ingestion.platform.framework.data.model.*;
import com.walmart.gmp.ingestion.platform.framework.data.model.impl.v2.index.V2ItemStatusIndexEntity;
import com.walmart.gmp.ingestion.platform.framework.data.model.impl.v2.index.V2ItemStatusIndexKey;
import com.walmart.gmp.ingestion.platform.framework.feed.FeedData.FeedStatus;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.Constants;
import com.walmart.marketplace.messages.v1_feedstatus.Feedstatus;
import com.walmart.partnerapi.common.message.FeedReplayFilter;
import com.walmart.partnerapi.common.message.PartnerReplayFeedMessage;
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

import static com.google.common.base.Preconditions.checkNotNull;
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
import static com.walmart.gmp.ingestion.platform.framework.utils.ConfigParser.parse;
import static com.walmart.gmp.ingestion.platform.framework.utils.PerfUtils.measure;
import static com.walmart.partnerapi.model.v2.ItemStatus.*;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getStackTraceString;
import static com.walmart.services.common.util.JsonUtil.convertToString;
import static java.lang.Class.forName;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Created by smalik3 on 8/24/16
 * Modified by cshah for adding Solr sync and inventory feed handling.
 */
public class FeedStatusAndCountsChecker implements EventProcessor<Event>, InitializingBean {

    public static final String FEED = "feed";
    public static final String ITEM = "item";
    public static final String PRICE = "price";
    public static final String ITEM_INDEX = "item_index";
    public static final String INVENTORY = "inventory";
    public static final String INVENTORY_INDEX = "inventory_index";
    public static final String SUPPLIER_ITEM = "supplier_item";
    public static final Selector<String, FeedStatusEntity> feedSelector =
            selector(FeedStatusEntity.class, e -> {
                e.getEntity_count();
                e.getSystemErrorCount();
                e.getTimeoutErrorCount();
                e.getSuccessCount();
                e.getDataErrorCount();
                e.getFeed_status();
            });
    private static final Logger L = Logger.getLogger(FeedStatusAndCountsChecker.class);
    private static final ListenableFuture<List<StrippedItemEntity>> DONE = immediateFuture(null);
    public final Map<String, DataManager<?, ?>> map = new HashMap<>();
    private final TaskExecutor taskExecutor = new TaskExecutor(retryableExceptions);
    AsyncManager itemAM;
    AsyncManager itemInventoryAM;
    AsyncManager itemPriceAM;
    AsyncManager supplierItemAM;
    private int fetchSize;
    private String replayUrl;
    private Map<String,String> replayHeaders;
    private static final AsyncHttpClient ASYNC_HTTP_CLIENT = new AsyncHttpClient();

    public FeedStatusAndCountsChecker(Map<String, Object> properties) throws Exception {
        createDM(properties.get("feedDMCCMPath"), FEED);
        createDM(properties.get("itemDMCCMPath"), ITEM);
        createDM(properties.get("priceItemDMCCMPath"), PRICE);
        createDM(properties.get("inventoryItemDMCCMPath"), INVENTORY);
        createDM(properties.get("itemIndexDMCCMPath"), ITEM_INDEX);
        createDM(properties.get("inventoryItemIndexDMCCMPath"), INVENTORY_INDEX);
        createDM(properties.get("supplierItemDMCCMPath"), SUPPLIER_ITEM);

        afterPropertiesSet();
    }

    public void createDM(Object ccmPath, Object dmName) throws Exception {
        final DataManagerConfig dataManagerConfig = parse(ccmPath.toString(), DataManagerConfig.class);
        if (dataManagerConfig.getConfigResolverBean() != null) { // Expects the canonical name of the ConfigResolver class
            L.info("dataManagerConfig has configResolved, trying to get bean from spring applicationContext. ConfigResolverBean Name: " + dataManagerConfig.getConfigResolverBean());
            ConfigResolver configBean = (ConfigResolver) forName(dataManagerConfig.getConfigResolverBean(), true, Thread.currentThread().getContextClassLoader()).newInstance();
            checkNotNull(configBean, "Unable to find ConfigResolverBean in applicationContext");
            final DataManager<Object, Entity<Object>> dm = new DataManager<>(dataManagerConfig, Collections.singletonMap(dataManagerConfig.getConfigResolverBean(), configBean));
            map.put(dmName.toString(), dm);
        } else {
            final DataManager<Object, Entity<Object>> dm = new DataManager<>(dataManagerConfig);
            map.put(dmName.toString(), dm);
        }
    }

    @SuppressWarnings("unchecked")
    public <K, T extends Entity<K>> DataManager<K, T> dm(String key) {
        return (DataManager<K, T>) map.get(key);
    }

    @Override
    public ListenableFuture<Event> process(Event event) {
        L.info(format("feedId: %s, processing event for status and counts: ", event.getXrefId()));
        String payload = event.getPayload();
        Feedstatus feedstatus = getEventFeedStatus(payload);
        FeedType feedType = getFeedType(feedstatus == null ? FeedType.ITEM.name() : feedstatus.getFeedType());
        final DataManager<String, FeedStatusEntity> feedDM = dm(FEED);
        final DataManager<FeedItemStatusKey, ItemStatusEntity> itemDM = dm(ITEM);

        try {
            final String feedId = event.getXrefId();
            return measure(feedId, L, () -> catching(transformAsync(feedDM.getAsync(feedId, feedSelector), fes -> {
                final FeedStatusEntity entity = entity(FeedStatusEntity.class, feedId);
                entity.setModified_by("BigBen");
                final int itemCount = fes.getEntity_count() == null ? 0 : parseInt(fes.getEntity_count());
                final String feedStatus = fes.getFeed_status();
                if (itemCount == 0) {
                    L.debug(format("feedId: %s has item count as 0", feedId));
                    if (!ERROR.name().equals(feedStatus) && !PROCESSED.name().equals(feedStatus)) {
                        L.warn(format("feedId: %s was not marked %s or %s, marking it as %s", feedId, ERROR, PROCESSED, ERROR));
                        entity.setFeed_status(ERROR.name());
                        entity.setSuccessCount(0);
                        entity.setSystemErrorCount(0);
                        entity.setDataErrorCount(0);
                        entity.setEntity_count(String.valueOf(itemCount));
                        entity.setTimeoutErrorCount(itemCount);
                        entity.setError_code(convertToString(createEntityCountGatewayError()));
                        entity.setError_message(convertToString(createEntityCountGatewayError()));
                        entity.setModified_dtm(new Date());
                        return transform(transform(feedDM.saveAsync(entity), (Function<FeedStatusEntity, ListenableFuture<FeedStatusEntity>>) $$ -> replayItems(entity,feedType)), (Function<ListenableFuture<FeedStatusEntity>, Event>) $$ -> event);
                    } else {
                        L.info(format("feedId: %s was already marked as %s, nothing to do", feedId, feedStatus));
                        return immediateFuture(event);
                    }
                }
                switch (FeedStatus.valueOf(feedStatus == null ? INPROGRESS.name() : feedStatus)) {
                    case PROCESSED:
                        L.info(format("feedId: %s is already marked PROCESSED, nothing to do", feedId));
                        return transform(replayItems(fes,feedType),(Function<FeedStatusEntity, Event>) $ -> event);
                    case RECEIVED:
                        L.warn(format("feedId: %s, is still in received status: %s, marking all items timed out", feedId, feedStatus));
                        entity.setSuccessCount(0);
                        entity.setSystemErrorCount(0);
                        entity.setDataErrorCount(0);
                        entity.setEntity_count(String.valueOf(0));
                        entity.setTimeoutErrorCount(itemCount);
                        entity.setError_code(convertToString(createGatewayError()));
                        entity.setError_message(convertToString(createGatewayError()));
                        entity.setFeed_status(ERROR.name());
                        entity.setModified_dtm(new Date());
                        return transform(transform(feedDM.saveAsync(entity), (Function<FeedStatusEntity, ListenableFuture<FeedStatusEntity>>) $$ -> replayItems(entity,feedType)), (Function<ListenableFuture<FeedStatusEntity>, Event>) $$ -> event);
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
                            return transform(transform(feedDM.saveAsync(entity), (Function<FeedStatusEntity, ListenableFuture<FeedStatusEntity>>) $$ -> replayItems(entity,feedType)), (Function<ListenableFuture<FeedStatusEntity>, Event>) $$ -> event);
                        });
                    case ERROR:
                        L.info(format("feedId: %s is already marked ERROR, nothing to do", feedId));
                        return transform(replayItems(fes,feedType),(Function<FeedStatusEntity, Event>) $ -> event);
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

    private ListenableFuture<FeedStatusEntity> replayItems(FeedStatusEntity entity, FeedType feedType) {

        try {
            if (!FeedType.SUPPLIER_FULL_ITEM.equals(feedType)) {
                L.info(format("no replay request for feedId: %s it is not a 1p feed.", entity.getFeed_id()));
                return immediateFuture(entity);
            }
            final int systemErrorCount = entity.getSystemErrorCount() != null ? entity.getSystemErrorCount() : 0;
            final int timeoutErrorCount = entity.getTimeoutErrorCount() != null ? entity.getTimeoutErrorCount() : 0;

            final PartnerReplayFeedMessage m = new PartnerReplayFeedMessage();
            m.setFeedID(entity.getFeed_id());
            final FeedReplayFilter f = new FeedReplayFilter();
            if(entity.getEntity_count() !=null &&  entity.getTimeoutErrorCount() !=null  && Integer.parseInt(entity.getEntity_count()) == entity.getTimeoutErrorCount()){
                f.setFilterName(FeedReplayFilter.ReplayFilterType.ALL);
                m.setFilter(asList(f));
            } else {
                f.setFilterName(FeedReplayFilter.ReplayFilterType.STATUS_FILTER);
                Set<String> filterValue = systemErrorCount > 0 && timeoutErrorCount > 0 ? new HashSet<>(asList(ItemStatus.TIMEOUT_ERROR.name(), ItemStatus.SYSTEM_ERROR.name())) : systemErrorCount > 0 ? new HashSet<>(asList(ItemStatus.SYSTEM_ERROR.name())) :  new HashSet<>(asList(ItemStatus.TIMEOUT_ERROR.name(), ItemStatus.SYSTEM_ERROR.name()));
                f.setFilterValue(filterValue);
                m.setFilter(asList(f));
            }

            final AsyncHttpClient.BoundRequestBuilder builder = ASYNC_HTTP_CLIENT.preparePut(replayUrl).setBody(convertToString(asList(m)));
            replayHeaders.forEach(builder::addHeader);

            builder.execute(new AsyncCompletionHandler<Response>() {
                @Override
                public Response onCompleted(Response response) throws Exception {
                    if (response.getStatusCode() == 200) {
                        if (L.isDebugEnabled())
                            L.debug(format("replay request submitted successfully for feed_id: %s ",entity.getFeed_id()));
                    } else {
                        L.warn(format("replay request failed for feed_id: %s  with status code: %s and error:  %s", entity.getFeed_id(),response.getStatusCode(), response.getResponseBody()));
                    }
                    return response;
                }

            });

        } catch (Exception e) {
            L.error(format("replay processing failed with % s", getRootCause(e)));
        }
        return immediateFuture(entity);
    }

    private FeedType getFeedType(String feedTypeStr) {

        if (StringUtils.equalsIgnoreCase(feedTypeStr, FeedType.INVENTORY.name())) {
            return FeedType.INVENTORY;
        } else if (StringUtils.equalsIgnoreCase(feedTypeStr, FeedType.LAGTIME.name())) {
            return FeedType.LAGTIME;
        }else if (StringUtils.equalsIgnoreCase(feedTypeStr, FeedType.PRICE.name()) || StringUtils.equalsIgnoreCase(feedTypeStr, FeedType.PROMO_PRICE.name()) ||
                StringUtils.equalsIgnoreCase(feedTypeStr, FeedType.CPT_PRICE.name()) || StringUtils.equalsIgnoreCase(feedTypeStr, FeedType.CPT_SELLER_ELIGIBILITY.name()) || StringUtils.equalsIgnoreCase(feedTypeStr, FeedType.MP_ITEM_PRICE_UPDATE.name())) {
            return FeedType.PRICE;
        } else if (StringUtils.equalsIgnoreCase(feedTypeStr, FeedType.SUPPLIER_FULL_ITEM.name())) {
            return FeedType.SUPPLIER_FULL_ITEM;
        } else {
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

    private GatewayError createEntityCountGatewayError() {
        GatewayError gatewayError = new GatewayError();
        gatewayError.setCode("ERR_PDI_0001");
        gatewayError.setType("SYSTEM_ERROR");
        gatewayError.setInfo("Feed SLA violated");
        gatewayError.setDescription("Feed had not received any item. Please check your file or reach out to operations team to (possibly) re-ingest this feed");
        gatewayError.setComponent("BigBen");
        return gatewayError;
    }

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

    private ListenableFuture<List<StrippedItemEntity>> calculateCounts0(String feedId, int shard, int itemIndex, Map<ItemStatus, Integer> counts, FeedType feedType) {
        ListenableFuture<List<StrippedItemDO>> f = immediateFuture(null);
        if (feedType != null && (feedType.equals(FeedType.INVENTORY) || feedType.equals(FeedType.LAGTIME))) {
            f = taskExecutor.async(() -> itemInventoryAM.sliceQuery(StrippedItemDO.class).forSelect().
                    withPartitionComponents(feedId, shard).fromClusterings(itemIndex).withExclusiveBounds().
                    limit(fetchSize).get(), "fetch-feed-items-inventory:" + feedId + "(" + itemIndex + ", Inf)", 3, 1000, 2, MILLISECONDS);
        } else if (feedType != null && feedType.equals(FeedType.PRICE)) {
            f = taskExecutor.async(() -> itemPriceAM.sliceQuery(StrippedItemDO.class).forSelect().
                    withPartitionComponents(feedId, shard).fromClusterings(itemIndex).withExclusiveBounds().
                    limit(fetchSize).get(), "fetch-feed-items:" + feedId + "(" + itemIndex + ", Inf)", 3, 1000, 2, MILLISECONDS);
        } else if (feedType != null && feedType.equals(FeedType.SUPPLIER_FULL_ITEM)) {
            f = taskExecutor.async(() -> supplierItemAM.sliceQuery(StrippedItemDO.class).forSelect().
                    withPartitionComponents(feedId, shard).fromClusterings(itemIndex).withExclusiveBounds().
                    limit(fetchSize).get(), "fetch-feed-items:" + feedId + "(" + itemIndex + ", Inf)", 3, 1000, 2, MILLISECONDS);
        } else {
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
        if (feedType != null && (feedType.equals(FeedType.INVENTORY) || feedType.equals(FeedType.LAGTIME))) {
            return collectInventoryItemEntity(inprogress, feedId);
        }
        return collectItemEntity(inprogress, feedId, feedType);
    }


    private ListenableFuture<List<StrippedItemEntity>> collectItemEntity(List<StrippedItemEntity> inprogress, String feedId, FeedType feedType) {
        return transform(allAsList(inprogress.stream().map(k -> {
            final ItemStatusEntity entity = entity(ItemStatusEntity.class,
                    FeedItemStatusKey.of(k.id().getFeedId(), k.id().getShard(), k.id().getSku(), k.id().getSkuIndex()));
            if (L.isDebugEnabled())
                L.debug(format("feedId: %s, marking item as timed out: %s", feedId, inprogress));
            entity.setItem_processing_status(TIMEOUT_ERROR.name());
            entity.setModified_dtm(new Date());
            return updateBothDBs(entity, feedId, feedType);
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
            return updateBothDBsInventory(entity, feedId);
        }).collect(toList())), (Function<List<InventoryFeedItemStatusEntity>, List<StrippedItemEntity>>) $ -> inprogress);
    }


    private ListenableFuture<InventoryFeedItemStatusEntity> updateBothDBsInventory(InventoryFeedItemStatusEntity entity, String feedId) {

        final DataManager<InventoryFeedItemStatusKey, InventoryFeedItemStatusEntity> itemDM = dm(INVENTORY);
        return itemDM.saveAsync(entity);
    }

    private ListenableFuture<ItemStatusEntity> updateBothDBs(ItemStatusEntity entity, String feedId, FeedType feedType) {
        final DataManager<FeedItemStatusKey, ItemStatusEntity> itemDM = dm(ITEM);
        final DataManager<FeedItemStatusKey, ItemStatusEntity> priceItemDM = dm(PRICE);
        final DataManager<FeedItemStatusKey, ItemStatusEntity> supplierItemDM = dm(SUPPLIER_ITEM);

        final V2ItemStatusIndexEntity v2entity = entity(V2ItemStatusIndexEntity.class,
                V2ItemStatusIndexKey.from(FeedItemStatusKey.of(entity.id().getFeedId(), entity.id().getShard(),
                        entity.id().getSku(), entity.id().getSkuIndex())));
        if (L.isDebugEnabled())
            L.debug(format("feedId: %s, syncing item status in in both DBs: %s", feedId, entity.id()));
        v2entity.setItem_processing_status(entity.getItem_processing_status());
        v2entity.setModified_dtm(new Date());
        if (feedType != null && feedType.equals(FeedType.PRICE)) {
            return priceItemDM.saveAsync(entity);
        } else if (feedType != null && feedType.equals(FeedType.SUPPLIER_FULL_ITEM)) {
            return supplierItemDM.saveAsync(entity);
        } else {
            return itemDM.saveAsync(entity);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        final DataManager<FeedItemStatusKey, ItemStatusEntity> itemDM = dm(ITEM);
        final DataManager<InventoryFeedItemStatusKey, InventoryFeedItemStatusEntity> itemInventoryDM = dm(INVENTORY);
        final DataManager<FeedItemStatusKey, ItemStatusEntity> priceItemDM = dm(PRICE);
        final DataManager<FeedItemStatusKey, ItemStatusEntity> supplierItemDM = dm(SUPPLIER_ITEM);
        final CqlDAO<?, ?> feedCqlDAO = (CqlDAO<?, ?>) itemDM.getPrimaryDAO(V2).unwrap();
        final CqlDAO<?, ?> feedCqlDAO0 = (CqlDAO<?, ?>) itemInventoryDM.getPrimaryDAO(V2).unwrap();
        final CqlDAO<?, ?> feedCqlDAOPrice = (CqlDAO<?, ?>) priceItemDM.getPrimaryDAO(V2).unwrap();
        final CqlDAO<?, ?> feedCqlDAOSupplierItem = (CqlDAO<?, ?>) supplierItemDM.getPrimaryDAO(V2).unwrap();
        itemInventoryAM = feedCqlDAO0.cqlDriverConfig().getAsyncPersistenceManager();
        itemPriceAM = feedCqlDAOPrice.cqlDriverConfig().getAsyncPersistenceManager();
        itemAM = feedCqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
        supplierItemAM = feedCqlDAOSupplierItem.cqlDriverConfig().getAsyncPersistenceManager();
        fetchSize = PROPS.getInteger("feed.items.fetch.size", 400);
        replayUrl =  PROPS.getProperty("replay.url");
        replayHeaders=  Constants.OBJECT_MAPPER.readValue(PROPS.getProperty("replay.headers"), HashMap.class);
    }

    private Feedstatus getEventFeedStatus(String payload) {
        try {
            Feedstatus feedstatus = JsonUtil.convertToObject(payload, Feedstatus.class);
            return feedstatus;
        } catch (Exception e) {
            L.error("Json conversion failed " + payload, e);
        }
        return null;
    }
}
