package com.walmartlabs.opensource.bigben.api

import com.google.common.util.concurrent.Futures.immediateFuture
import com.google.common.util.concurrent.ListenableFuture
import com.hazelcast.map.EntryBackupProcessor
import com.hazelcast.map.EntryProcessor
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.walmartlabs.opensource.bigben.core.ScheduleScanner.Companion.BUCKET_CACHE
import com.walmartlabs.opensource.bigben.entities.*
import com.walmartlabs.opensource.bigben.entities.EventStatus.*
import com.walmartlabs.opensource.bigben.extns.*
import com.walmartlabs.opensource.bigben.hz.HzObjectFactory.OBJECT_ID.EVENT_RECEIVER_ADD_EVENT
import com.walmartlabs.opensource.bigben.processors.ProcessorRegistry
import com.walmartlabs.opensource.core.*
import com.walmartlabs.opensource.core.hz.Hz
import com.walmartlabs.opensource.core.utils.Props
import toResponse
import java.time.ZonedDateTime
import java.util.*
import kotlin.collections.MutableMap.MutableEntry

/**
 * Created by smalik3 on 2/26/18
 */
class EventReceiver(val hz: Hz) {

    companion object {
        private val l = logger<EventReceiver>()
        internal val CACHED_PROCESSOR = CountIncrementer()
    }

    private val scanInterval = Props.int("event.schedule.scan.interval.minutes", 1)
    private val lapseOffset = Props.int("event.lapse.offset.minutes", 0)

    init {
        if (l.isInfoEnabled) l.info("using event lapseOffset: {} minutes", lapseOffset)
    }

    fun addEvent(eventRequest: EventRequest): ListenableFuture<EventResponse> {
        return validate(eventRequest)?.let { it } ?: {
            val eventTime = ZonedDateTime.parse(eventRequest.eventTime)
            val bucketId = utc(bucketize(eventTime.toInstant().toEpochMilli(), scanInterval))
            fetch<EventLookup> { it.xrefId = eventRequest.id; it.tenant = eventRequest.tenant }
                    .transformAsync {
                        if (it != null) {
                            if (it.eventTime == eventTime) {
                                if (l.isDebugEnabled) l.debug("{}, event update received, no change in event time", eventRequest.id)
                                save<EventLookup> { it.xrefId = eventRequest.id; it.tenant = eventRequest.tenant; it.payload = eventRequest.payload }.transform {
                                    if (l.isDebugEnabled) l.debug("{}, event updated successfully", eventRequest.id)
                                    eventRequest.toResponse().apply { eventId = it!!.eventId; eventStatus = UPDATED }
                                }
                            } else {
                                if (l.isDebugEnabled) l.debug("event update received, event time changed, add new event -> update existing look up -> delete old event")
                                val oldLookup = it
                                addEvent0(eventRequest, bucketId, eventTime).transformAsync {
                                    addLookup0(eventRequest, bucketId, it!!.shard!!, it.id!!, eventTime).transformAsync { removeEvent0(oldLookup) }.transform {
                                        eventRequest.toResponse().apply { eventId = it!!.eventId; eventStatus = UPDATED }
                                    }
                                }
                            }
                        } else {
                            addEvent0(eventRequest, bucketId, eventTime).transformAsync {
                                addLookup0(eventRequest, it!!.bucketId!!, it.shard!!, it.id!!, it.eventTime!!).transform {
                                    if (l.isDebugEnabled) l.debug("{}, add-event: successful", it!!.xrefId)
                                    eventRequest.toResponse().apply { eventId = it!!.eventId; eventStatus = ACCEPTED }
                                }
                            }
                        }
                    }.catching {
                l.error("failed to add event: {}", eventRequest.id, it.rootCause())
                eventRequest.toResponse().apply { eventStatus = ERROR }
            }
        }()
    }

    private fun addLookup0(eventRequest: EventRequest, bucketId: ZonedDateTime, shard: Int, eventId: String, eventTime: ZonedDateTime): ListenableFuture<EventLookup> {
        return save {
            it.tenant = eventRequest.tenant
            it.xrefId = eventRequest.id
            it.bucketId = bucketId
            it.shard = shard
            it.eventTime = eventTime
            it.eventId = eventId
            it.payload = eventRequest.payload
            if (l.isDebugEnabled) l.debug("{}, add-event: event-lookup-table: insert", eventRequest.id)
        }
    }

    private fun addEvent0(eventRequest: EventRequest, bucketId: ZonedDateTime, eventTime: ZonedDateTime): ListenableFuture<Event> {
        return hz.hz.getMap<ZonedDateTime, Bucket>(BUCKET_CACHE).let {
            it.submitToKey(bucketId, CACHED_PROCESSOR).listenable().transformAsync {
                val count = it as Long
                save<Event> {
                    if (l.isDebugEnabled) l.debug("{}, add-event: event-table: insert", eventRequest.id)
                    it.id = UUID.randomUUID().toString()
                    it.eventTime = eventTime
                    it.shard = ((count - 1) / Props.int("event.shard.size", 1000)).toInt()
                    it.status = UN_PROCESSED
                    it.tenant = eventRequest.tenant
                    it.xrefId = eventRequest.id
                    it.bucketId = eventTime.bucket()
                }
            }
        }
    }

    private fun removeEvent0(eventLookup: EventLookup): ListenableFuture<EventLookup> {
        return { remove<Event> { it.eventTime = eventLookup.eventTime; it.id = eventLookup.eventId; it.shard = eventLookup.shard; it.bucketId = eventLookup.bucketId } }.
                retriable("delete-event-${eventLookup.xrefId}",
                        Props.int("event.delete.max.retries", 3),
                        Props.int("event.delete.initial.delay", 1),
                        Props.int("event.delete.backoff.multiplier", 2)
                ).transform { eventLookup }
    }

    fun removeEvent(id: String, tenant: String): ListenableFuture<EventResponse> {
        val eventResponse = EventResponse().apply { this.id = id; this.tenant = tenant }
        return fetch<EventLookup> { it.xrefId = id; it.tenant = tenant }.transformAsync { el ->
            if (el == null) immediateFuture(eventResponse)
            else {
                if (l.isDebugEnabled) l.debug("removing event: {}/{}", tenant, id)
                remove<Event> { it.eventTime = el.eventTime; it.shard = el.shard; it.id = el.eventId; it.bucketId = el.bucketId }.transformAsync {
                    if (l.isDebugEnabled) l.debug("removing event look up: {}/{}", tenant, id)
                    remove<EventLookup> { it.tenant = el.tenant; it.xrefId = el.xrefId }.transform {
                        if (l.isDebugEnabled) l.debug("event removed successfully : {}/{}", tenant, id)
                        eventResponse.apply { eventStatus = DELETED; eventId = it?.eventId }
                    }
                }
            }
        }.catching {
            l.error("error in removing the event: {}/{}", tenant, id, it.rootCause())
            //eventResponse.setErrors(newArrayList<Error>(Error("500", id, cause.message, getStackTraceString(cause), ERROR, APPLICATION)))
            eventResponse.apply { eventStatus = ERROR }
        }
    }

    private fun validate(eventRequest: EventRequest): ListenableFuture<EventResponse>? {
        if (eventRequest.tenant == null) {
            val eventResponse = eventRequest.toResponse()
            eventResponse.eventStatus = REJECTED
            //eventResponse.error = (newArrayList<Error>(Error("400", "tenant", "", "tenant not present")))
            l.error("event rejected, tenant missing, {}", eventRequest.json())
            return immediateFuture<EventResponse>(eventResponse)
        }
        if (eventRequest.eventTime == null) {
            val eventResponse = eventRequest.toResponse()
            eventResponse.eventStatus = REJECTED
            //eventResponse.setErrors(newArrayList<Error>(Error("400", "eventTime", "", "event time not present")))
            l.error("event rejected, event time not present, {} ", eventRequest.json())
            return immediateFuture<EventResponse>(eventResponse)
        }
        if (eventRequest.tenant!! !in ProcessorRegistry.instance.registeredTenants()) {
            if (Props.string("skip.tenant.validation", "no") == "no") {
                val eventResponse = eventRequest.toResponse()
                eventResponse.eventStatus = REJECTED
                //eventResponse.setErrors(newArrayList<Error>(Error("400", "tenant", "", "tenant not registered / unknown tenant")))
                l.error("event rejected, unknown tenant. Did you register one in the processors.config?, {}", eventRequest.json())
                return immediateFuture<EventResponse>(eventResponse)
            }
        }
        try {
            ZonedDateTime.parse(eventRequest.eventTime)
        } catch (e: Exception) {
            val eventResponse = eventRequest.toResponse()
            eventResponse.eventStatus = REJECTED
            //eventResponse.setErrors(newArrayList<Error>(Error("400", "eventTime", "", "event time can not be parsed. Must be in ISO 8601 format.")))
            l.error("event rejected, bad event time format, {}", eventRequest.json())
            return immediateFuture<EventResponse>(eventResponse)
        }

        if (ZonedDateTime.parse(eventRequest.eventTime).isBefore(nowUTC().plusMinutes(lapseOffset.toLong()))) {
            val eventResponse = eventRequest.toResponse()
            eventResponse.eventStatus = TRIGGERED
            eventResponse.triggeredAt = nowUTC().toString()
            if (l.isDebugEnabled) l.debug("lapsed event received, marking it {}, eventRequest: {}", TRIGGERED, eventRequest.json())
            return immediateFuture<EventResponse>(eventResponse)
        }
        return null
    }

    internal class CountIncrementer :
            Idso(EVENT_RECEIVER_ADD_EVENT), EntryProcessor<ZonedDateTime, Bucket?>, EntryBackupProcessor<ZonedDateTime, Bucket?> {

        companion object {
            private val l = logger<CountIncrementer>()
        }

        override fun getBackupProcessor() = this

        override fun processBackup(entry: MutableEntry<ZonedDateTime, Bucket?>?) {
            process(entry!!)
        }

        override fun process(entry: MutableEntry<ZonedDateTime, Bucket?>): Long? {
            val b = if (entry.value == null) domainProvider<Bucket>().let { it.raw(it.selector(Bucket::class.java)) } else entry.value!!
            b.count = (b.count ?: 0) + 1L
            b.updatedAt = nowUTC()
            if (b.status == null)
                b.status = UN_PROCESSED
            entry.setValue(b)
            if (l.isDebugEnabled) l.debug("bucket-id: {}, old-count: {}, new-count: {} ", entry.key, b.count!! - 1, b.count)
            return b.count
        }

        override fun writeData(out: ObjectDataOutput) {
        }

        override fun readData(`in`: ObjectDataInput) {
        }
    }
}