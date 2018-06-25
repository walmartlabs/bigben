package com.walmartlabs.bigben.entities

import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.extns.epoch
import java.time.ZonedDateTime

/**
 * Created by smalik3 on 2/25/18
 */
interface EntityProvider<T> {
    fun selector(type: Class<T>): T
    fun raw(selector: T): T
    fun fetch(selector: T): ListenableFuture<T?>
    fun kvs(selector: KV): ListenableFuture<List<KV>>
    fun save(selector: T): ListenableFuture<T>
    fun remove(selector: T): ListenableFuture<T>
    fun unwrap(): Any?
}

interface EventLoader {
    fun load(bucketId: ZonedDateTime, shard: Int, fetchSize: Int, eventTime: ZonedDateTime = epoch(), eventId: String = "", context: Any? = null): ListenableFuture<Pair<Any?, List<Event>>>
}