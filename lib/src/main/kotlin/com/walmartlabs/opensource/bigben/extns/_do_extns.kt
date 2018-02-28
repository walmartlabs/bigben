package com.walmartlabs.opensource.bigben.extns

import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.opensource.bigben.entities.EntityProvider
import java.util.*

fun <T> fullSelector(t: T) {}

val provider = ServiceLoader.load(EntityProvider::class.java).take(1)[0]!!

@Suppress("UNCHECKED_CAST")
inline fun <reified T> provider() = provider as EntityProvider<T>

inline fun <reified T> fetch(selector: (T) -> Unit = ::fullSelector): ListenableFuture<T?> {
    return provider<T>().let { it.fetch(it.selector(T::class.java).apply { selector(this) }) }
}

inline fun <reified T> save(selector: (T) -> Unit): ListenableFuture<T> {
    return provider<T>().let { it.save(it.selector(T::class.java).apply { selector(this) }) }
}

inline fun <reified T> remove(selector: (T) -> Unit): ListenableFuture<T> {
    return provider<T>().let { it.remove(it.selector(T::class.java).apply { selector(this) }) }
}