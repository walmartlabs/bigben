package com.walmart.gmp.feeds;

import com.walmart.gmp.ingestion.platform.framework.data.model.FeedItemStatusKey;
import com.walmart.gmp.ingestion.platform.framework.data.model.impl.v2.item.V2FeedItemStatusKey;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Entity;
import org.apache.solr.client.solrj.beans.Field;

/**
 * Created by smalik3 on 8/25/16
 */
@Entity(table = "feed_item_status")
public class StrippedItemDO implements StrippedItemEntity, com.walmart.gmp.ingestion.platform.framework.data.core.Entity<V2FeedItemStatusKey> {

    @EmbeddedId
    private V2FeedItemStatusKey id = new V2FeedItemStatusKey();

    @Field("item_processing_status")
    @Column(name = "item_processing_status")
    private String item_processing_status;

    public V2FeedItemStatusKey getId() {
        return id;
    }

    public void setId(V2FeedItemStatusKey id) {
        this.id = id;
    }

    public String getItem_processing_status() {
        return item_processing_status;
    }

    public void setItem_processing_status(String item_processing_status) {
        this.item_processing_status = item_processing_status;
    }

    @Override
    public String toString() {
        return "ItemDO{" +
                "id=" + id +
                ", item_processing_status='" + item_processing_status + '\'' +
                '}';
    }


    @Override
    public V2FeedItemStatusKey id() {
        return id;
    }

    public Object key() {
        return  id ;
    }
}
