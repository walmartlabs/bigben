package com.walmartlabs.opensource.bigben.providers.domain.cassandra

import com.datastax.driver.mapping.Mapper
import com.datastax.driver.mapping.MappingManager
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.opensource.bigben.entities.Bucket
import com.walmartlabs.opensource.bigben.entities.EntityProvider
import com.walmartlabs.opensource.bigben.entities.Event
import com.walmartlabs.opensource.bigben.entities.EventLookup
import com.walmartlabs.opensource.bigben.extns.transform
import com.walmartlabs.opensource.bigben.tasks.EventLoader

/**
 * Created by smalik3 on 3/2/18
 */
open class CassandraProvider : EntityProvider<Any> {

    private lateinit var mappingManager: MappingManager

    override fun loader(): EventLoader<Any> = TODO("later")

    override fun selector(type: Class<Any>): Any {
        return when (type) {
            Event::class.java -> EventC()
            Bucket::class.java -> BucketC()
            EventLookup::class.java -> EventLookupC()
            else -> IllegalArgumentException("unknown entity $type")
        }
    }

    override fun raw(selector: Any) = selector

    override fun fetch(selector: Any): ListenableFuture<Any?> {
        return mappingManager.mapper(selector::class.java).let {
            when (selector) {
                is EventC -> {
                    require(selector.eventTime != null && selector.id != null &&
                            selector.shard != null && selector.shard!! > 0) { "event keys not provided: $selector" }
                    it.getAsync(selector.bucketId, selector.shard, selector.eventTime, selector.id).transform { _ -> it }
                }
                is BucketC -> {
                    require(selector.id != null) { "bucket id not provided: $selector" }
                    it.getAsync(selector.id).transform { _ -> it }
                }
                is EventLookupC -> {
                    require(selector.tenant != null && selector.xrefId != null) { "look up keys not provided: $selector" }
                    it.getAsync(selector.tenant, selector.xrefId).transform { _ -> it }
                }
                else -> throw IllegalArgumentException("unknown selector: $selector")
            }
        }
    }

    override fun save(selector: Any): ListenableFuture<Any> {
        return mappingManager.mapper(selector::class.java).let {
            @Suppress("UNCHECKED_CAST")
            val m = it as Mapper<Any>
            when (selector) {
                is EventC -> {
                    require(selector.eventTime != null && selector.id != null &&
                            selector.shard != null && selector.shard!! > 0) { "event keys not provided: $selector" }
                }
                is BucketC -> {
                    require(selector.id != null) { "bucket id not provided: $selector" }
                }
                is EventLookupC -> {
                    require(selector.tenant != null && selector.xrefId != null) { "look up keys not provided: $selector" }
                }
                else -> throw IllegalArgumentException("unknown selector: $selector")
            }
            m.saveAsync(selector).transform { _ -> selector }
        }
    }

    override fun remove(selector: Any): ListenableFuture<Any> {
        TODO("later")
    }
}