package com.walmartlabs.opensource.core.utils

import com.google.common.util.concurrent.ListenableFuture
import com.hazelcast.core.ExecutionCallback
import com.hazelcast.core.ICompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * Created by smalik3 on 2/23/18
 */
class ListenableFutureAdapter<T>(private val delegate: ICompletableFuture<T>) : ListenableFuture<T> {

    override fun addListener(listener: Runnable, executor: Executor) {
        delegate.andThen(object : ExecutionCallback<T> {
            override fun onResponse(response: T) {
                listener.run()
            }

            override fun onFailure(t: Throwable) {
                // no-op
            }
        }, executor)
    }

    override fun cancel(mayInterruptIfRunning: Boolean) = delegate.cancel(mayInterruptIfRunning)
    override fun isCancelled() = delegate.isCancelled
    override fun isDone() = delegate.isDone

    @Throws(InterruptedException::class, ExecutionException::class)
    override fun get() = delegate.get()
    @Throws(InterruptedException::class, ExecutionException::class, TimeoutException::class)
    override operator fun get(timeout: Long, unit: TimeUnit) = delegate.get(timeout, unit)
}