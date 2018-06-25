package com.walmartlabs.bigben.extns

import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.BigBen.entityProvider
import com.walmartlabs.bigben.entities.KV

inline fun <reified T> fetch(selector: (T) -> Unit): ListenableFuture<T?> {
    return entityProvider<T>().let { it.fetch(it.selector(T::class.java).apply { selector(this) }) }
}

inline fun kvs(selector: (KV) -> Unit): ListenableFuture<List<KV>> {
    return entityProvider<KV>().let { it.kvs(it.selector(KV::class.java).apply { selector(this) }) }
}

inline fun <reified T> save(selector: (T) -> Unit): ListenableFuture<T> {
    return entityProvider<T>().let { it.save(it.selector(T::class.java).apply { selector(this) }) }
}

inline fun <reified T> remove(selector: (T) -> Unit): ListenableFuture<T> {
    return entityProvider<T>().let { it.remove(it.selector(T::class.java).apply { selector(this) }) }
}