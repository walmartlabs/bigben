package com.walmartlabs.opensource.bigben.core

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.opensource.bigben.entities.EventLookup
import com.walmartlabs.opensource.bigben.extns.fetch
import com.walmartlabs.opensource.bigben.extns.save
import com.walmartlabs.opensource.core.*
import java.time.ZonedDateTime
import java.util.*
import java.util.stream.Collectors

internal class CheckpointHelper {
    companion object {
        private val l = logger<CheckpointHelper>()
        private val CHECKPOINT_KEY = "_CHECKPOINT_"
    }

    @JsonInclude(NON_EMPTY)
    data class Checkpoint(val b: String, val c: Long, val a: List<Int> = emptyList(), val p: List<Int> = emptyList())

    fun saveCheckpoint(data: Map<ZonedDateTime, BucketSnapshot>): ListenableFuture<EventLookup> {
        return data.toSortedMap().mapValues {
            Checkpoint(it.value.id.toString(), it.value.count,
                    it.value.awaiting.stream().boxed().collect(Collectors.toList()),
                    it.value.processing.stream().boxed().collect(Collectors.toList()))
        }.let { m ->
            if (l.isDebugEnabled) l.debug("saving checkpoint for buckets: {}", m.keys)
            save<EventLookup> { it.tenant = CHECKPOINT_KEY; it.xrefId = CHECKPOINT_KEY; it.payload = m.values.json() }
        }.done({ l.warn("error in saving checkpoint", it.rootCause()) }) {
            if (l.isDebugEnabled && it != null) l.debug("checkpoint saved successfully for buckets: {}", it.payload)
        }
    }

    fun loadCheckpoint(): ListenableFuture<Map<ZonedDateTime, BucketSnapshot>> {
        return fetch<EventLookup> { it.tenant = CHECKPOINT_KEY; it.xrefId = CHECKPOINT_KEY }.transform {
            when {
                it?.payload != null && it.payload!!.trim().isNotEmpty() -> {
                    typeRefJson<List<Checkpoint>>(it.payload!!).map {
                        val awaiting = it.a.fold(BitSet(), { b, i -> b.apply { set(i) } })
                        val processing = it.p.fold(BitSet(), { b, i -> b.apply { set(i) } })
                        processing.stream().forEach { awaiting.set(it) }
                        BucketSnapshot(ZonedDateTime.parse(it.b), it.c, BitSet(), awaiting)
                    }.associate { it.id to it }.also { if (l.isDebugEnabled) l.debug("loaded checkpoint: {}", it) }
                }
                else -> {
                    if (l.isInfoEnabled)
                        l.info("no checkpoint to load")
                    HashMap()
                }
            }
        }.catching { l.error("error in loading checkpoint, ignoring", it.rootCause()); HashMap() }
    }
}