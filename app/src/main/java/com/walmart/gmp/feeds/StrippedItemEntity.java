package com.walmart.gmp.feeds;

import com.walmart.gmp.ingestion.platform.framework.data.core.Entity;
import com.walmart.gmp.ingestion.platform.framework.data.model.FeedItemStatusKey;
import com.walmart.gmp.ingestion.platform.framework.data.model.ItemStatusEntity;
import com.walmart.gmp.ingestion.platform.framework.data.model.impl.v2.item.V2FeedItemStatusKey;
import info.archinnov.achilles.annotations.Column;
import org.apache.solr.client.solrj.beans.Field;

/**
 * Created by cshah on 9/25/16.
 */

public interface StrippedItemEntity  extends Entity<V2FeedItemStatusKey> {


    String getItem_processing_status();

    void setItem_processing_status(String item_processing_status);
}
