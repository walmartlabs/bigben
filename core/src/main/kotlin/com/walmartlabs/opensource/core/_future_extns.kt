package com.walmartlabs.opensource.core

import com.google.common.base.Function
import com.google.common.base.Throwables.getStackTraceAsString
import com.google.common.util.concurrent.*
import com.google.common.util.concurrent.MoreExecutors.directExecutor
import com.hazelcast.core.ICompletableFuture
import com.walmartlabs.opensource.core.utils.ListenableFutureAdapter
import com.walmartlabs.opensource.core.utils.Props
import com.walmartlabs.opensource.core.utils.TaskExecutor
import org.slf4j.LoggerFactory
import java.util.UUID.randomUUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS

/**
 * Created by smalik3 on 2/21/18
 */
val l = LoggerFactory.getLogger("extns")

fun onError(t: Throwable?) = l.error("error in processing: ${getStackTraceAsString(t.rootCause()!!)}", t.rootCause())

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

fun <T> List<ListenableFuture<T>>.reduce(): ListenableFuture<List<T>> {
    return Futures.allAsList(this)
}

fun <T, R> ListenableFuture<T>.transform(t: (T?) -> R): ListenableFuture<R> {
    return Futures.transform(this, Function { t(it) }, directExecutor())
}

fun <T> ListenableFuture<T>.catching(t: (Throwable?) -> T): ListenableFuture<T> {
    return Futures.catching(this, Exception::class.java, Function { t(it) }, directExecutor())
}

fun <T> ListenableFuture<T>.catchingAsync(t: (Throwable?) -> ListenableFuture<T>): ListenableFuture<T> {
    return Futures.catchingAsync(this, Exception::class.java, AsyncFunction { t(it) }, directExecutor())
}

fun <T, R> ListenableFuture<T>.transformAsync(t: (T?) -> ListenableFuture<R>): ListenableFuture<R> {
    return Futures.transformAsync(this, AsyncFunction { t(it) }, directExecutor())
}

fun <T> AsyncCallable<T>.scheduleAsync(delay: Long, units: TimeUnit, scheduledExecutor: ScheduledExecutorService): ListenableFuture<T> {
    return Futures.scheduleAsync(this, delay, units, scheduledExecutor)
}

fun <T> AsyncCallable<T>.submitAsync(executorService: ExecutorService): ListenableFuture<T> {
    return Futures.submitAsync(this, executorService)
}

private val te = TaskExecutor(setOf(Exception::class.java))

fun <T> (() -> ListenableFuture<T>).retriable(taskId: String = randomUUID().toString(),
                                              maxRetries: Int = Props.int("task.executor.max.retries", 3),
                                              delay: Int = Props.int("task.executor.delay", 1),
                                              backoffMultiplier: Int = Props.int("task.executor.backoff.multiplier", 2),
                                              timeUnit: TimeUnit = SECONDS,
                                              taskExecutor: TaskExecutor = te): ListenableFuture<T> =
        taskExecutor.async(taskId, maxRetries, delay, backoffMultiplier, timeUnit, this)

@Suppress("UNCHECKED_CAST")
fun <T, F : Future<T>> F.listenable(): ListenableFutureAdapter<T> {
    require(this is ICompletableFuture<*>) { "future must be instance of ICompletableFuture" }
    return ListenableFutureAdapter(this as ICompletableFuture<T>)
}

fun <T> Future<T>.result(waitTime: Long = Props.long("generic.future.max.get.time", 60L), errorHandler: (Exception) -> T?): T {
    return try {
        get(waitTime, SECONDS)
    } catch (e: Exception) {
        errorHandler(e) ?: throw e
    }
}