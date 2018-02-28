package com.walmartlabs.opensource.bigben.entities

import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.opensource.bigben.tasks.EventLoader

/**
 * Created by smalik3 on 2/25/18
 */
interface EntityProvider<T> {
    fun loader(): EventLoader<T>
    fun selector(type: Class<T>): T
    fun raw(selector: T): T
    fun fetch(selector: T): ListenableFuture<T?>
    fun save(selector: T): ListenableFuture<T>
    fun remove(selector: T): ListenableFuture<T>
}