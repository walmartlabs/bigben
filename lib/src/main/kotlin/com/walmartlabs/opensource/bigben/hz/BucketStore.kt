package com.walmartlabs.opensource.bigben.hz

import com.hazelcast.core.MapStore
import com.walmartlabs.opensource.bigben.entities.Bucket
import com.walmartlabs.opensource.bigben.extns.*
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit.MINUTES

/**
 * Created by smalik3 on 3/3/18
 */
class BucketStore : MapStore<ZonedDateTime, Bucket> {

    private val l = logger<BucketStore>()

    private val provider = domainProvider<Bucket>()

    override fun deleteAll(keys: MutableCollection<ZonedDateTime>?) {
        throw UnsupportedOperationException("not supported")
    }

    override fun load(key: ZonedDateTime): Bucket? {
        return { fetch<Bucket> { it.id = key } }.retriable("load-bucket: $key").get(1, MINUTES)
    }

    override fun loadAll(keys: Collection<ZonedDateTime>): Map<ZonedDateTime, Bucket> {
        return keys.map { k -> { fetch<Bucket> { it.id = k } }.retriable("load-bucket: $k") }.reduce().get(1, MINUTES).associate { it!!.id!! to it }
    }

    override fun storeAll(map: Map<ZonedDateTime, Bucket>) {
        map.values.map { v -> { provider.save(v) }.retriable("save-bucket: ${v.id}") }
    }

    override fun loadAllKeys(): Iterable<ZonedDateTime>? = null

    override fun delete(key: ZonedDateTime) {
        throw UnsupportedOperationException("not supported")
    }

    override fun store(key: ZonedDateTime, value: Bucket) {
        if (l.isDebugEnabled) l.debug("saving bucket: {}", key);
        { provider.save(value.apply { id = key }) }.retriable("save-bucket: $key").get(1, MINUTES)
    }
}