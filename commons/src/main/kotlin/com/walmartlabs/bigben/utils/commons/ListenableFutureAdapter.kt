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
package com.walmartlabs.bigben.utils.commons

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
                if (t is ExecutionException && t.cause == null) {
                    t.initCause(RuntimeException(t.message))
                    setException(RuntimeException(t))
                } else setException(t)
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
