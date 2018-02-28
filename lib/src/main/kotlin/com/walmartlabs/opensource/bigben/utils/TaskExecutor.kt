package com.walmartlabs.opensource.bigben.utils

import com.fasterxml.jackson.databind.util.ClassUtil.getRootCause
import com.google.common.util.concurrent.Futures.immediateFailedFuture
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors.listeningDecorator
import com.walmartlabs.opensource.bigben.extns.catchingAsync
import com.walmartlabs.opensource.bigben.extns.logger
import com.walmartlabs.opensource.bigben.extns.transformAsync
import java.lang.Integer.getInteger
import java.lang.Runtime.getRuntime
import java.lang.String.format
import java.util.UUID.randomUUID
import java.util.concurrent.*
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

/**
 * Created by smalik3 on 2/22/18
 */
class TaskExecutor(private val retryableExceptions: Set<Class<*>>) {

    companion object {
        private val l = logger<TaskExecutor>()
        private val serial = AtomicInteger()
        private val RETRY_POOL = listeningDecorator(ScheduledThreadPoolExecutor(getInteger("task.executor.retry.thread.count", getRuntime().availableProcessors()),
                ThreadFactory { r -> Thread(r, "task-executor-retry-worker#" + serial.getAndIncrement()) }, ThreadPoolExecutor.CallerRunsPolicy()))
    }

    fun <R> async(taskId: String = randomUUID().toString(), maxRetries: Int = Props.int("task.executor.max.retries", 3), delay: Int = Props.int("task.executor.delay", 1000),
                  backoffMultiplier: Int = Props.int("task.executor.backoff.multiplier", 2), timeUnit: TimeUnit = SECONDS, task: () -> ListenableFuture<R>): ListenableFuture<R> {
        return async(taskId, maxRetries, delay, backoffMultiplier, timeUnit, Supplier { Callable { task() } })
    }

    fun <R> async(taskId: String = randomUUID().toString(), maxRetries: Int = Props.int("task.executor.max.retries", 3), delay: Int = Props.int("task.executor.delay", 1000),
                  backoffMultiplier: Int = Props.int("task.executor.backoff.multiplier", 2), timeUnit: TimeUnit = SECONDS, supplier: Supplier<Callable<ListenableFuture<R>>>): ListenableFuture<R> {
        return async(taskId, 0, maxRetries, delay, backoffMultiplier, timeUnit, supplier)
    }

    private fun <R> async(taskId: String, retryCount: Int, maxRetries: Int,
                          delay: Int, backoffMultiplier: Int, timeUnit: TimeUnit, task: Supplier<Callable<ListenableFuture<R>>>): ListenableFuture<R> {
        return try {
            task.get().call().catchingAsync { mayBeRetry(task, taskId, retryCount, maxRetries, delay, backoffMultiplier, timeUnit, it) }
        } catch (t: Throwable) {
            mayBeRetry(task, taskId, retryCount, maxRetries, delay, backoffMultiplier, timeUnit, t)
        }
    }


    private fun <R> mayBeRetry(task: Supplier<Callable<ListenableFuture<R>>>, taskId: String, retryCount: Int, maxRetries: Int,
                               delay: Int, backoffMultiplier: Int, timeUnit: TimeUnit, t: Throwable): ListenableFuture<R> {
        val cause = getRootCause(t)
        return if (shouldRetry(cause)) {
            if (retryCount < maxRetries) {
                if (l.isWarnEnabled)
                    l.warn(format("operation failed, taskId='{}', retrying after {} {}, retry={}, maxRetry={}, exception='{}'",
                            taskId, delay, timeUnit, retryCount, maxRetries, if (cause.message == null) cause::class.java.name else cause.message))
                RETRY_POOL.schedule(Callable { async(taskId, retryCount + 1, maxRetries, backoffMultiplier * delay, backoffMultiplier, timeUnit, task) }, delay.toLong(), timeUnit).transformAsync { it -> it }
            } else {
                l.error(format("operation failed, taskId='{}', after {} retries, will not be retried anymore, exception='{}'",
                        taskId, maxRetries, if (cause.message == null) cause::class.java.name else cause.message), cause)
                immediateFailedFuture<R>(cause)
            }
        } else {
            l.error(format("operation failed, taskId='{}', unexpected exception", taskId), cause)
            immediateFailedFuture<R>(cause)
        }
    }

    private fun shouldRetry(cause: Throwable?) = cause?.let { retryableExceptions.find { t -> t.isAssignableFrom(cause::class.java) } != null } ?: false
}