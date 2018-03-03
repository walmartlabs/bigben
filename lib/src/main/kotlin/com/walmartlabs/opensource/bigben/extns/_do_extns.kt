package com.walmartlabs.opensource.bigben.extns

import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.opensource.bigben.entities.EntityProvider
import java.util.*

val domainProvider = try {
    ServiceLoader.load(EntityProvider::class.java).first()!!.also {
        if (l.isInfoEnabled) l.info("domain provider loaded successfully")
    }
} catch (e: Throwable) {
    l.error("could not load domain provider", e.rootCause())
    throw ExceptionInInitializerError(e.rootCause())
}

@Suppress("UNCHECKED_CAST")
inline fun <reified T> domainProvider() = domainProvider as EntityProvider<T>

inline fun <reified T> fetch(selector: (T) -> Unit): ListenableFuture<T?> {
    return domainProvider<T>().let { it.fetch(it.selector(T::class.java).apply { selector(this) }) }
}

inline fun <reified T> save(selector: (T) -> Unit): ListenableFuture<T> {
    return domainProvider<T>().let { it.save(it.selector(T::class.java).apply { selector(this) }) }
}

inline fun <reified T> remove(selector: (T) -> Unit): ListenableFuture<T> {
    return domainProvider<T>().let { it.remove(it.selector(T::class.java).apply { selector(this) }) }
}