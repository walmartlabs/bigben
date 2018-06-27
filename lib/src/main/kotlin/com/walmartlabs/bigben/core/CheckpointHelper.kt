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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.entities.KV
import com.walmartlabs.bigben.extns.fetch
import com.walmartlabs.bigben.extns.save
import com.walmartlabs.bigben.utils.*
import java.time.ZonedDateTime
import java.util.*
import java.util.stream.Collectors

internal class CheckpointHelper {
    companion object {
        private val l = logger<CheckpointHelper>()
        private const val CHECKPOINT_KEY = "_CHECKPOINT_"
    }

    @JsonInclude(NON_EMPTY)
    data class Checkpoint(val b: String, val c: Long, val a: List<Int> = emptyList(), val p: List<Int> = emptyList())

    fun saveCheckpoint(data: Map<ZonedDateTime, BucketSnapshot>): ListenableFuture<KV> {
        return data.toSortedMap().mapValues {
            Checkpoint(it.value.id.toString(), it.value.count,
                    it.value.awaiting.stream().boxed().collect(Collectors.toList()),
                    it.value.processing.stream().boxed().collect(Collectors.toList()))
        }.let { m ->
            if (l.isDebugEnabled) l.debug("saving checkpoint for buckets: {}", m.keys)
            save<KV> { it.key = CHECKPOINT_KEY; it.column = ""; it.value = m.values.json() }
        }.done({ l.warn("error in saving checkpoint", it.rootCause()) }) {
            if (l.isDebugEnabled && it != null) l.debug("checkpoint saved successfully for buckets: {}", it.value)
        }
    }

    fun loadCheckpoint(): ListenableFuture<out Map<ZonedDateTime, BucketSnapshot>> {
        return fetch<KV> { it.key = CHECKPOINT_KEY; it.column = "" }.transform {
            it?.value?.run {
                typeRefJson<List<Checkpoint>>(this).map {
                    val awaiting = it.a.fold(BitSet()) { b, i -> b.apply { set(i) } }
                    val processing = it.p.fold(BitSet()) { b, i -> b.apply { set(i) } }
                    processing.stream().forEach { awaiting.set(it) }
                    BucketSnapshot(ZonedDateTime.parse(it.b), it.c, BitSet(), awaiting)
                }.associate { it.id to it }.also { if (l.isDebugEnabled) l.debug("loaded checkpoint: {}", it) }
            } ?: if (l.isInfoEnabled) l.info("no checkpoint to load"); HashMap<ZonedDateTime, BucketSnapshot>()
        }.catching { l.error("error in loading checkpoint", it.rootCause()); HashMap() }
    }
}
