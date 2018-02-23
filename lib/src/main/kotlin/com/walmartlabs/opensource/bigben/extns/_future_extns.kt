package com.walmartlabs.opensource.bigben.extns

import com.google.common.base.Function
import com.google.common.base.Throwables.getRootCause
import com.google.common.base.Throwables.getStackTraceAsString
import com.google.common.util.concurrent.AsyncFunction
import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors.directExecutor
import com.walmartlabs.opensource.bigben.entities.utils.Props
import com.walmartlabs.opensource.bigben.entities.utils.TaskExecutor
import org.slf4j.LoggerFactory
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS

/**
 * Created by smalik3 on 2/21/18
 */
private val l = LoggerFactory.getLogger("FutureExtensions")

fun onError(t: Throwable?) {
    l.error("error in processing: ${getStackTraceAsString(getRootCause(t))} ")
}

fun <T> ListenableFuture<T>.done(onError: (Throwable?) -> Unit = ::onError, onSuccess: (T?) -> Unit): ListenableFuture<T> {
    return also {
        Futures.addCallback(it, object : FutureCallback<T> {
            override fun onFailure(t: Throwable?) {
                onError(t)
            }

            override fun onSuccess(result: T?) {
                onSuccess(result)
            }
        }, directExecutor())
    }
}

fun <T> List<ListenableFuture<T>>.done(onError: (Throwable?) -> Unit = ::onError, onSuccess: (List<T>?) -> Unit): ListenableFuture<List<T>> {
    return reduce().done(onError, onSuccess)
}

fun <T> List<ListenableFuture<out T>>.reduce(): ListenableFuture<List<T>> {
    return Futures.allAsList(this)
}

fun <T, R> ListenableFuture<out T>.transform(t: (T?) -> R): ListenableFuture<out R> {
    return Futures.transform(this as ListenableFuture<T>, Function { t(it) }, directExecutor())
}

fun <T> ListenableFuture<out T>.catching(t: (Throwable) -> T): ListenableFuture<T> {
    return Futures.catching(this, Exception::class.java, Function { t(it) }, directExecutor())
}

fun <T> ListenableFuture<out T>.catchingAsync(t: (Throwable) -> ListenableFuture<T>): ListenableFuture<T> {
    return Futures.catchingAsync(this, Exception::class.java, AsyncFunction { t(it) }, directExecutor())
}

fun <T, R> ListenableFuture<out T>.transformAsync(t: (T) -> ListenableFuture<R>): ListenableFuture<out R> {
    return Futures.transformAsync(this as ListenableFuture<T>, AsyncFunction { t(it) }, directExecutor())
}

private val te = TaskExecutor(setOf(Exception::class.java))

fun <T> (() -> ListenableFuture<T>).retriable(taskId: String = randomUUID().toString(),
                                              maxRetries: Int = Props.int("task.executor.max.retries", 3),
                                              delay: Int = Props.int("task.executor.delay", 1000),
                                              backoffMultiplier: Int = Props.int("task.executor.backoff.multiplier", 2),
                                              timeUnit: TimeUnit = SECONDS,
                                              taskExecutor: TaskExecutor = te): ListenableFuture<T> =
        taskExecutor.async(taskId, maxRetries, delay, backoffMultiplier, timeUnit, this)