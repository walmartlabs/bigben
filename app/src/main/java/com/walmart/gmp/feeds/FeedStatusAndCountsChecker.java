package com.walmart.gmp.feeds;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmart.gmp.ingestion.platform.framework.data.model.FeedItemStatusKey;
import com.walmart.gmp.ingestion.platform.framework.data.model.FeedStatusEntity;
import com.walmart.gmp.ingestion.platform.framework.data.model.ItemStatusEntity;
import com.walmart.gmp.ingestion.platform.framework.feed.FeedData.FeedStatus;
import com.walmart.partnerapi.model.v2.ItemStatus;
import com.walmart.services.nosql.data.CqlDAO;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.processors.EventProcessor;
import info.archinnov.achilles.persistence.AsyncManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.util.concurrent.Futures.*;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.selector;
import static com.walmart.gmp.ingestion.platform.framework.feed.FeedData.FeedStatus.*;
import static com.walmart.gmp.ingestion.platform.framework.feed.FeedData.FeedStatus.INPROGRESS;
import static com.walmart.partnerapi.model.v2.ItemStatus.*;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getStackTraceString;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * Created by smalik3 on 8/24/16
 */
public class FeedStatusAndCountsChecker implements EventProcessor<Event>, InitializingBean {

    private static final Logger L = Logger.getLogger(FeedStatusAndCountsChecker.class);

    private AsyncManager itemAM;

    private int fetchSize;

    private final Selector<String, FeedStatusEntity> feedSelector =
            selector(FeedStatusEntity.class, e -> {
                e.getEntity_count();
                e.getSystemErrorCount();
                e.getTimeoutErrorCount();
                e.getSuccessCount();
                e.getDataErrorCount();
                e.getFeed_status();
            });

    private final Map<String, DataManager<?, ?>> map;

    public FeedStatusAndCountsChecker(Map<String, DataManager<?, ?>> map) {
        this.map = map;
    }

    @SuppressWarnings("unchecked")
    private <K, T extends Entity<K>> DataManager<K, T> dm(String key) {
        return (DataManager<K, T>) map.get(key);
    }

    @Override
    public ListenableFuture<Event> process(Event event) {
        L.debug("processing feed event for status and counts: " + event.getPayload());
        final DataManager<String, FeedStatusEntity> feedDM = dm("feed");
        try {
            final String feedId = event.getXrefId();
            return transformAsync(feedDM.getAsync(feedId, feedSelector), fes -> {
                final FeedStatusEntity entity = entity(FeedStatusEntity.class, feedId);
                entity.setModified_by("BigBen");
                final int itemCount = fes.getEntity_count() == null ? 0 : parseInt(fes.getEntity_count());
                final String feedStatus = fes.getFeed_status();
                if (itemCount == 0) {
                    L.info(format("feed %s has item count as 0", feedId));
                    if (!ERROR.name().equals(feedStatus) || !PROCESSED.name().equals(feedStatus)) {
                        L.warn(format("feed %s was not marked %s or %s, marking it as %s", feedId, ERROR, PROCESSED, ERROR));
                        entity.setFeed_status(ERROR.name());
                        entity.setModified_dtm(new Date());
                        return transform(feedDM.saveAsync(entity), (Function<FeedStatusEntity, Event>) $ -> event);
                    } else {
                        L.info(format("feed %s was already marked as %s, nothing to do", feedId, feedStatus));
                        return immediateFuture(event);
                    }
                }
                switch (FeedStatus.valueOf(feedStatus == null ? INPROGRESS.name() : feedStatus)) {
                    case PROCESSED:
                        L.debug("feed is already marked processed, nothing to do");
                        return immediateFuture(event);
                    case RECEIVED:
                        L.warn(format("feed %s, is still in received status: %s, marking all items timed out", feedId, ERROR));
                        entity.setSuccessCount(0);
                        entity.setSystemErrorCount(0);
                        entity.setDataErrorCount(0);
                        entity.setTimeoutErrorCount(itemCount);
                        entity.setFeed_status(PROCESSED.name());
                        entity.setModified_dtm(new Date());
                        return transform(feedDM.saveAsync(entity), (Function<FeedStatusEntity, Event>) $ -> event);
                    case INPROGRESS:
                        L.warn(format("feed %s, is still in progress status: %s, initiating the time out procedure", feedId, ERROR));
                        final Map<ItemStatus, Integer> countsMap = new HashMap<>();
                        return transformAsync(calculateCounts(feedId, -1, countsMap), $ -> {
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
                                L.warn(format("feed: %s, was done, but counts were not synced up, syncing them, counts are: %s", feedId, countsMap));
                                entity.setTimeoutErrorCount(countsMap.get(TIMEOUT_ERROR));
                            } else {
                                L.info(format("feed: %s has timed out, marking the final status and counts: %s", feedId, countsMap));
                                entity.setTimeoutErrorCount(itemCount - nonTimeOuts);
                            }
                            entity.setModified_dtm(new Date());
                            return transform(feedDM.saveAsync(entity), (Function<FeedStatusEntity, Event>) $$ -> event);
                        });
                    default:
                        throw new IllegalArgumentException(format("unknown feed status: %s for feedId: %s", feedStatus, feedId));
                }
            });
        } catch (Exception e) {
            final Throwable cause = getRootCause(e);
            L.error("error in processing event: ", cause);
            event.setError(getStackTraceString(cause));
            return immediateFuture(event);
        }
    }

    private static final ListenableFuture<List<StrippedItemDO>> DONE = immediateFuture(null);

    private ListenableFuture<List<StrippedItemDO>> calculateCounts(String feedId, int itemIndex, Map<ItemStatus, Integer> counts) {
        final DataManager<FeedItemStatusKey, ItemStatusEntity> itemDM = dm("item");
        @SuppressWarnings("unchecked")
        final ListenableFuture<List<StrippedItemDO>> f =
                itemDM.async(() -> itemAM.sliceQuery(StrippedItemDO.class).forSelect().
                        withPartitionComponents(feedId).fromClusterings(itemIndex).withExclusiveBounds().
                        limit(fetchSize).get(), "fetch-feed-items:" + feedId + "(" + itemIndex + ", Inf)");
        return transformAsync(f, l -> {
            final List<StrippedItemDO> inprogress = l.stream().filter(e -> "INPROGRESS".equals(e.getItem_processing_status())).collect(toList());
            if (!inprogress.isEmpty()) {
                L.warn(format("feedId: %s, following items are still in progress, marking them timed out: " + inprogress, feedId));
            }
            l.forEach(e -> {
                final ItemStatus status = ItemStatus.valueOf(e.getItem_processing_status());
                if (!counts.containsKey(status))
                    counts.put(status, 0);
                counts.put(status, counts.get(status) + 1);
            });
            if (l.isEmpty() || l.size() < fetchSize) {
                return inprogress.isEmpty() ? DONE : tof(inprogress);
            } else
                return inprogress.isEmpty() ? calculateCounts(feedId, l.get(l.size() - 1).getId().getSkuIndex(), counts) :
                        transform(allAsList(calculateCounts(feedId, l.get(l.size() - 1).getId().getSkuIndex(), counts), tof(inprogress)),
                                new Function<List<List<StrippedItemDO>>, List<StrippedItemDO>>() {
                                    @Override
                                    public List<StrippedItemDO> apply(List<List<StrippedItemDO>> ll) {
                                        return ll.get(0);
                                    }
                                });
        });
    }

    private ListenableFuture<List<StrippedItemDO>> tof(List<StrippedItemDO> inprogress) {
        return transform(allAsList(inprogress.stream().map(k -> {
            final ItemStatusEntity entity = entity(ItemStatusEntity.class,
                    FeedItemStatusKey.of(k.getId().getFeedId(), k.getId().getShard(), k.getId().getSku(), k.getId().getSkuIndex()));
            L.debug("marking item as timed out: " + k);
            entity.setItem_processing_status(TIMEOUT_ERROR.name());
            entity.setModified_dtm(new Date());
            final DataManager<FeedItemStatusKey, ItemStatusEntity> itemDM = dm("item");
            return itemDM.saveAsync(entity);
        }).collect(toList())), (Function<List<ItemStatusEntity>, List<StrippedItemDO>>) $ -> inprogress);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        final DataManager<FeedItemStatusKey, ItemStatusEntity> itemDM = dm("item");
        final CqlDAO<?, ?> feedCqlDAO = (CqlDAO<?, ?>) itemDM.unwrap();
        itemAM = feedCqlDAO.cqlDriverConfig().getAsyncPersistenceManager();
        fetchSize = PROPS.getInteger("feed.items.fetch.size");
    }
}
