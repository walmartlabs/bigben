/*-
 * #%L
 * BigBen:commons
 * =======================================
 * Copyright (C) 2016 - 2018 Walmart Inc.
 * =======================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.walmartlabs.bigben.utils.utils

import com.fasterxml.jackson.databind.util.ClassUtil.getRootCause
import com.google.common.util.concurrent.Futures.immediateFailedFuture
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors.listeningDecorator
import com.walmartlabs.bigben.utils.catchingAsync
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.transformAsync
import com.walmartlabs.bigben.utils.utils.Props.int
import java.lang.Integer.getInteger
import java.lang.Runtime.getRuntime
import java.util.UUID.randomUUID
import java.util.concurrent.*
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

/**
 * Created by smalik3 on 2/22/18
 */
class TaskExecutor(private val logErrorStackDuringIntermediateRetries: Boolean = false, private val isRetriable: (t: Throwable) -> Boolean) {
    constructor(retriableExceptions: Set<Class<*>>, logErrorStackDuringIntermediateRetries: Boolean = false) : this(logErrorStackDuringIntermediateRetries,
            fun(t: Throwable?): Boolean {
                return t?.let { retriableExceptions.find { t -> t.isAssignableFrom(t::class.java) } != null }
                        ?: false
            })

    companion object {
        private val l = logger<TaskExecutor>()
        private val serial = AtomicInteger()
        private val RETRY_POOL = listeningDecorator(ScheduledThreadPoolExecutor(int("task.executor.retry.thread.count", getRuntime().availableProcessors()),
                ThreadFactory { r -> Thread(r, "task-executor-retry-worker#" + serial.getAndIncrement()) }, ThreadPoolExecutor.CallerRunsPolicy()))
    }

    fun <R> async(taskId: String = randomUUID().toString(), maxRetries: Int = int("task.executor.max.retries"), delay: Int = int("task.executor.delay"),
                  backoffMultiplier: Int = int("task.executor.backoff.multiplier"), timeUnit: TimeUnit = SECONDS, task: () -> ListenableFuture<R>): ListenableFuture<R> {
        return async(taskId, maxRetries, delay, backoffMultiplier, timeUnit, Supplier { Callable { task() } })
    }

    fun <R> async(taskId: String = randomUUID().toString(), maxRetries: Int = int("task.executor.max.retries"), delay: Int = int("task.executor.delay"),
                  backoffMultiplier: Int = int("task.executor.backoff.multiplier"), timeUnit: TimeUnit = SECONDS, supplier: Supplier<Callable<ListenableFuture<R>>>): ListenableFuture<R> {
        return async(taskId, 0, maxRetries, delay, backoffMultiplier, timeUnit, supplier)
    }

    private fun <R> async(taskId: String, retryCount: Int, maxRetries: Int,
                          delay: Int, backoffMultiplier: Int, timeUnit: TimeUnit, task: Supplier<Callable<ListenableFuture<R>>>): ListenableFuture<R> {
        return try {
            task.get().call().catchingAsync { mayBeRetry(task, taskId, retryCount, maxRetries, delay, backoffMultiplier, timeUnit, it!!) }
        } catch (t: Throwable) {
            mayBeRetry(task, taskId, retryCount, maxRetries, delay, backoffMultiplier, timeUnit, t)
        }
    }

    private fun <R> mayBeRetry(task: Supplier<Callable<ListenableFuture<R>>>, taskId: String, retryCount: Int, maxRetries: Int,
                               delay: Int, backoffMultiplier: Int, timeUnit: TimeUnit, t: Throwable): ListenableFuture<R> {
        val cause = getRootCause(t)
        return if (isRetriable(cause)) {
            if (retryCount < maxRetries) {
                if (l.isWarnEnabled) {
                    if (logErrorStackDuringIntermediateRetries) l.warn("operation failed, taskId='{}', retrying after {} {}, retry={}, maxRetry={}, exception='{}'",
                            taskId, delay, timeUnit, retryCount, maxRetries, if (cause.message == null) cause::class.java.name else cause.message)
                    else l.warn("operation failed, taskId='{}', retrying after {} {}, retry={}, maxRetry={}, exception='{}'",
                            taskId, delay, timeUnit, retryCount, maxRetries, if (cause.message == null) cause::class.java.name else cause.message, cause)
                }
                RETRY_POOL.schedule(Callable { async(taskId, retryCount + 1, maxRetries, backoffMultiplier * delay, backoffMultiplier, timeUnit, task) }, delay.toLong(), timeUnit).transformAsync { it -> it!! }
            } else {
                l.error("operation failed, taskId='{}', after {} retries, will not be retried anymore, exception='{}'",
                        taskId, maxRetries, if (cause.message == null) cause::class.java.name else cause.message, cause)
                immediateFailedFuture<R>(cause)
            }
        } else {
            l.error("operation failed, taskId='{}', unexpected exception", taskId, cause)
            immediateFailedFuture<R>(cause)
        }
    }
}
