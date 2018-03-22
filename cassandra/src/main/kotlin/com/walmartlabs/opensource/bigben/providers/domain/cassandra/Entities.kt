package com.walmartlabs.opensource.bigben.providers.domain.cassandra

import com.datastax.driver.mapping.annotations.*
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.walmartlabs.opensource.bigben.entities.*
import com.walmartlabs.opensource.bigben.extns.utc
import com.walmartlabs.opensource.bigben.hz.HzObjectFactory.Companion.BIGBEN_FACTORY_ID
import com.walmartlabs.opensource.bigben.hz.HzObjectFactory.OBJECT_ID.BUCKET
import java.time.ZonedDateTime
import java.util.*

/**
 * Created by smalik3 on 2/26/18
 */
@Table(keyspace = "bigben", name = "buckets")
data class BucketC(@PartitionKey @Column(name = "id") override var bucketId: ZonedDateTime? = null,
                   override var status: EventStatus? = null,
                   override var count: Long? = null,
                   @Column(name = "processed_at") override var processedAt: ZonedDateTime? = null,
                   @Column(name = "modified_at") override var updatedAt: ZonedDateTime? = null) : Bucket {
    @Transient override fun getFactoryId() = BIGBEN_FACTORY_ID
    @Transient override fun getId() = BUCKET.ordinal
    override fun writeData(out: ObjectDataOutput) {
        BitSet(4).apply {
            set(0, bucketId != null)
            set(1, status != null)
            set(2, count != null)
            set(3, processedAt != null)
            set(4, updatedAt != null)
        }.also { out.writeByteArray(it.toByteArray()) }.apply {
            if (get(0)) out.writeLong(bucketId!!.toInstant().toEpochMilli())
            if (get(1)) out.writeByte(status!!.ordinal)
            if (get(2)) out.writeLong(count!!)
            if (get(3)) out.writeLong(processedAt!!.toInstant().toEpochMilli())
            if (get(4)) out.writeLong(updatedAt!!.toInstant().toEpochMilli())
        }
    }

    override fun readData(ins: ObjectDataInput) {
        BitSet.valueOf(ins.readByteArray()).apply {
            if (get(0)) bucketId = utc(ins.readLong())
            if (get(1)) status = EventStatus.values()[ins.readByte().toInt()]
            if (get(2)) count = ins.readLong()
            if (get(3)) processedAt = utc(ins.readLong())
            if (get(4)) updatedAt = utc(ins.readLong())
        }
    }
}

@Table(keyspace = "bigben", name = "events")
data class EventC(@ClusteringColumn @Column(name = "event_time") override var eventTime: ZonedDateTime? = null,
                  @ClusteringColumn(1) override var id: String? = null,
                  @PartitionKey @Column(name = "bucket_id") override var bucketId: ZonedDateTime? = null,
                  @PartitionKey(1) override var shard: Int? = null,
                  override var status: EventStatus? = null,
                  override var error: String? = null,
                  override var tenant: String? = null,
                  @Column(name = "xref_id") override var xrefId: String? = null,
                  @Column(name = "processed_at") override var processedAt: ZonedDateTime? = null,
                  override var payload: String? = null,
                  @Transient override var eventResponse: EventResponse? = null) : Event

@Table(keyspace = "bigben", name = "lookups")
data class EventLookupC(@PartitionKey override var tenant: String? = null,
                        @PartitionKey(1) @Column(name = "xref_id") override var xrefId: String? = null,
                        @Column(name = "bucket_id") override var bucketId: ZonedDateTime? = null,
                        override var shard: Int? = null,
                        @Column(name = "event_time") override var eventTime: ZonedDateTime? = null,
                        @Column(name = "event_id") override var eventId: String? = null,
                        override var payload: String? = null) : EventLookup
