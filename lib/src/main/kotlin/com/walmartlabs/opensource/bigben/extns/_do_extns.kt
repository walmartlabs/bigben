package com.walmartlabs.opensource.bigben.extns

import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.opensource.bigben.entities.EventLookup

object DOManager {
    fun fetch(tenant: String, xrefId: String): ListenableFuture<EventLookup?> {

    }
}

fun EventLookup.save(): ListenableFuture<EventLookup> {
    return null
}

fun EventLookup.fetch(): ListenableFuture<EventLookup> {
    return null
}

fun <T> fullSelector(t: T) {}

inline fun <reified T> fetch(key: Any, selector: (T) -> Unit = ::fullSelector): ListenableFuture<T?> {
    return null
}

inline fun <reified T> save(t: (T) -> Unit): ListenableFuture<T> {
    t(null)
}