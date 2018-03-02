package com.walmartlabs.opensource.bigben.extns

import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.opensource.bigben.entities.EntityProvider
import java.util.*

val provider = try {
    ServiceLoader.load(EntityProvider::class.java).take(1)[0]!!.also {
        if (l.isInfoEnabled) l.info("domain provider loaded successfully")
    }
} catch (e: Exception) {
    l.error("could not load domain provider", e.rootCause())
}

@Suppress("UNCHECKED_CAST")
inline fun <reified T> provider() = provider as EntityProvider<T>

inline fun <reified T> fetch(selector: (T) -> Unit): ListenableFuture<T?> {
    return provider<T>().let { it.fetch(it.selector(T::class.java).apply { selector(this) }) }
}

inline fun <reified T> save(selector: (T) -> Unit): ListenableFuture<T> {
    return provider<T>().let { it.save(it.selector(T::class.java).apply { selector(this) }) }
}

inline fun <reified T> remove(selector: (T) -> Unit): ListenableFuture<T> {
    return provider<T>().let { it.remove(it.selector(T::class.java).apply { selector(this) }) }
}