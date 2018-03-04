package com.walmartlabs.opensource.bigben.entities

import com.google.common.util.concurrent.ListenableFuture
import java.time.ZonedDateTime

/**
 * Created by smalik3 on 2/25/18
 */
interface EntityProvider<T> {
    fun selector(type: Class<T>): T
    fun raw(selector: T): T
    fun fetch(selector: T): ListenableFuture<T?>
    fun save(selector: T): ListenableFuture<T>
    fun remove(selector: T): ListenableFuture<T>
}

interface EventLoader<T> {
    fun load(t: T?, bucketId: ZonedDateTime, shard: Int, fetchSize: Int): ListenableFuture<Pair<T?, List<Event>?>>
}