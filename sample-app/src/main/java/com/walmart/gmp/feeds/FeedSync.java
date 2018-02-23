package com.walmart.gmp.feeds;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.walmart.gmp.ingestion.platform.framework.core.FeedType;
import com.walmart.gmp.ingestion.platform.framework.data.core.DataManager;
import com.walmart.gmp.ingestion.platform.framework.data.core.Selector;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmart.gmp.ingestion.platform.framework.data.model.FeedItemStatusKey;
import com.walmart.gmp.ingestion.platform.framework.data.model.FeedStatusEntity;
import com.walmart.gmp.ingestion.platform.framework.data.model.ItemStatusEntity;
import com.walmart.marketplace.messages.v1_feedstatus.Feedstatus;
import com.walmart.services.common.util.JsonUtil;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.processors.EventProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;

import java.util.*;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.entity;
import static com.walmart.gmp.ingestion.platform.framework.data.core.DataManager.retryableExceptions;
import static com.walmart.gmp.ingestion.platform.framework.data.core.Selector.selector;
import static com.walmart.gmp.ingestion.platform.framework.utils.PerfUtils.measure;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getStackTraceString;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Created by cshah on 9/26/16.
 */
public class FeedSync implements EventProcessor<Event>, InitializingBean {

    private static final Logger L = Logger.getLogger(FeedSync.class);
    private int fetchSize;

    private final Selector<FeedItemStatusKey, ItemStatusEntity> itemSelector =
            selector(ItemStatusEntity.class, e -> {
                e.getItem_processing_state();
                e.getItem_processing_status();
                e.getErrorCode();
                e.getItemProcessingType();
            });

    FeedStatusAndCountsChecker feedStatusAndCountsChecker;

    public FeedSync(Map<String, Object> properties) throws Exception {
        feedStatusAndCountsChecker = new FeedStatusAndCountsChecker(properties);
        afterPropertiesSet();
    }

    @Override
    public ListenableFuture<Event> process(Event event) {

        L.info(format("feedId: %s, processing event for sync between cass and index: ", event.getXrefId()));
        String payload = event.getPayload();
        Feedstatus feedstatus =  getEventFeedStatus(payload);
        final String feedId = event.getXrefId();
        final DataManager<String, FeedStatusEntity> feedDM = feedStatusAndCountsChecker.dm(FeedStatusAndCountsChecker.FEED);
        FeedType feedType = getFeedType(feedstatus == null ? FeedType.ITEM.name() : feedstatus.getFeedType());
        return measure(feedId, L, () -> catching(transformAsync(feedDM.getAsync(feedId, FeedStatusAndCountsChecker.feedSelector), fes -> {
            final int itemCount = fes.getEntity_count() == null ? 0 : parseInt(fes.getEntity_count());
            return transformAsync(processItems(feedId,itemCount,PROPS.getInteger("feeds.shard.size", 1000),feedType) , $ -> {
                final FeedStatusEntity entity = entity(FeedStatusEntity.class, feedId);
                entity.setModified_by("BigBen-FeedSync");
                entity.setDataErrorCount(fes.getDataErrorCount());
                entity.setSuccessCount(fes.getSuccessCount());
                entity.setTimeoutErrorCount(fes.getTimeoutErrorCount());
                entity.setSystemErrorCount(fes.getSystemErrorCount());
                entity.setFeed_status(fes.getFeed_status());
                entity.setModified_dtm(new Date());
                final String feedStatus = fes.getFeed_status();
                return transform(feedDM.saveAsync(entity), (Function<FeedStatusEntity, Event>) $1 -> event);
                });
        }),Exception.class, ex -> {
            final Throwable cause = getRootCause(ex);
            if (retryableExceptions.contains(cause.getClass())) {
                L.warn(format("feedId: %s, event failed with a retryable error" + event, event.getXrefId()), cause);
                event.setError(getStackTraceString(cause));
            } else
                L.error(format("feedId: %s, event failed with a non-retryable error, will not be tried again: " + event, event.getXrefId()), cause);
            return event;
        }), "feed-status-count-checker");
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


    private ListenableFuture<List<StrippedItemEntity>> processItems(String feedId, int itemCount, int shardSize, FeedType feedType) {
        int numShards = itemCount % shardSize == 0 ? itemCount / shardSize : 1 + itemCount / shardSize;
        final List<Integer> shards = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            shards.add(i);
        }
        L.info(format("feedId: %s, calculating counts, spanning over shards: %s", feedId, shards));
        return transform(allAsList(shards.stream().map(shard -> collectItems(feedId, shard, -1, feedType)).filter(out -> out != null).collect(toList())),
                (Function<List<List<StrippedItemEntity>>, List<StrippedItemEntity>>) ll -> {
                    if (ll == null) {
                        return null;
                    } else {
                        return newArrayList(concat(ll.stream().filter(strippedItemDOs -> strippedItemDOs != null).collect(toList())));
                    }
                });
    }

    private final TaskExecutor taskExecutor = new TaskExecutor(retryableExceptions);

    private ListenableFuture<List<StrippedItemEntity>> collectItems(String feedId, int shard, int itemIndex, FeedType feedType) {
        ListenableFuture<List<StrippedItemDO>> f = immediateFuture(null);
        if(feedType != null && feedType.equals(FeedType.INVENTORY)) {
            f = taskExecutor.async(() -> feedStatusAndCountsChecker.itemInventoryAM.sliceQuery(StrippedItemDO.class).forSelect().
                    withPartitionComponents(feedId, shard).fromClusterings(itemIndex).withExclusiveBounds().
                    limit(fetchSize).get(), "fetch-feed-items-inventory:" + feedId + "(" + itemIndex + ", Inf)", 3, 1000, 2, MILLISECONDS);
        }
        else {
            f = taskExecutor.async(() -> feedStatusAndCountsChecker.itemAM.sliceQuery(StrippedItemDO.class).forSelect().
                    withPartitionComponents(feedId, shard).fromClusterings(itemIndex).withExclusiveBounds().
                    limit(fetchSize).get(), "fetch-feed-items:" + feedId + "(" + itemIndex + ", Inf)", 3, 1000, 2, MILLISECONDS);
        }
        return updateItems(feedId, shard, feedType, f);
    }

    private ListenableFuture<List<StrippedItemEntity>> updateItems(String feedId, int shard, FeedType feedType, ListenableFuture<List<StrippedItemDO>> f) {
//         transformAsync(f, l -> {
//            final List<StrippedItemEntity> items = l.stream().collect(toList());
//        //    return feedStatusAndCountsChecker.collectItemEntity(items, feedType);
//            });
        return immediateFuture(null);
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

    @Override
    public void afterPropertiesSet() throws Exception {

    }
}
