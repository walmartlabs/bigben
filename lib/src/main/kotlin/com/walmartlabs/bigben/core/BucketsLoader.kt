/*-
 * #%L
 * BigBen:lib
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
package com.walmartlabs.bigben.core

import com.google.common.util.concurrent.ListenableScheduledFuture
import com.walmartlabs.bigben.core.BucketManager.Companion.scheduler
import com.walmartlabs.bigben.entities.Bucket
import com.walmartlabs.bigben.extns.fetch
import com.walmartlabs.bigben.utils.commons.Props
import com.walmartlabs.bigben.utils.commons.TaskExecutor
import com.walmartlabs.bigben.utils.done
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.rootCause
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Created by smalik3 on 2/22/18
 */
class BucketsLoader(private val lookbackRange: Int, private val fetchSize: Int,
                    private val bucketWidth: Int, private val bucketId: ZonedDateTime, private val consumer: (Bucket) -> Unit) : Runnable {

    companion object {
        private val l = logger<BucketsLoader>()
    }

    private val waitInterval = Props.int("buckets.background.load.wait.interval.seconds")
    private val runningJob = AtomicReference<ListenableScheduledFuture<*>>()
    private val taskExecutor = TaskExecutor(setOf(Exception::class.java))

    override fun run() {
        l.info("starting the background load of buckets at a rate of {} buckets per {} seconds until {} buckets are loaded", fetchSize, waitInterval, lookbackRange)
        runningJob.set(scheduler.schedule({ load(0) }, 0, SECONDS))
    }

    private fun load(fromIndex: Int) {
        if (fromIndex >= lookbackRange) {
            if (l.isInfoEnabled) l.info("lookback range reached, bucket loading is finished")
        } else {
            if (l.isInfoEnabled) l.info("initiating background load of buckets from index: {}", fromIndex)
            val currentBucketIndex = AtomicReference<Int>()
            val atLeastOne = AtomicBoolean()
            (1..fetchSize).forEach {
                val bucketIndex = fromIndex + it
                if (bucketIndex <= lookbackRange) {
                    currentBucketIndex.set(bucketIndex)
                    val bId = bucketId.minusSeconds((bucketIndex * bucketWidth).toLong())
                    if (l.isDebugEnabled) l.debug("loading bucket: {}, failures will be retried {} times, every {} seconds", bId, lookbackRange - bucketIndex + 1, bucketWidth)
                    taskExecutor.async("bucket-load:$bId", lookbackRange - bucketIndex + 1, bucketWidth, 1) { fetch<Bucket> { it.bucketId = bId } }
                            .done({ l.error("error in loading bucket {}, system is giving up", bId, it.rootCause()) }) {
                                if (l.isDebugEnabled) l.debug("bucket {} loaded successfully", bId)
                                consumer(it.apply { atLeastOne.set(true) } ?: BucketManager.emptyBucket(bId))
                            }
                }
            }
            runningJob.set(scheduler.schedule({ load(currentBucketIndex.get()) }, (if (!atLeastOne.get()) 0 else waitInterval).toLong(), SECONDS))
        }
    }
}
