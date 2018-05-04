package com.walmartlabs.opensource.core.utils

import com.google.common.util.concurrent.AbstractFuture
import com.hazelcast.core.ExecutionCallback
import com.hazelcast.core.ICompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * Created by smalik3 on 2/23/18
 */
class ListenableFutureAdapter<T>(private val delegate: ICompletableFuture<T>) : AbstractFuture<T>() {

    override fun addListener(listener: Runnable, executor: Executor) {
        super.addListener(listener, executor)
        delegate.andThen(object : ExecutionCallback<T> {
            override fun onResponse(response: T) {
                set(response)
            }

            override fun onFailure(t: Throwable) {
                setException(t)
            }
        }, executor)
    }

    override fun cancel(mayInterruptIfRunning: Boolean) = super.cancel(mayInterruptIfRunning).run { delegate.cancel(mayInterruptIfRunning) }
    override fun isCancelled() = delegate.isCancelled
    override fun isDone() = delegate.isDone

    @Throws(InterruptedException::class, ExecutionException::class)
    override fun get(): T? = delegate.get()

    @Throws(InterruptedException::class, ExecutionException::class, TimeoutException::class)
    override operator fun get(timeout: Long, unit: TimeUnit): T? = delegate.get(timeout, unit)
}