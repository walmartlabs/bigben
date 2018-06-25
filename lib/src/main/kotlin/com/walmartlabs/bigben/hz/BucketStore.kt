package com.walmartlabs.bigben.hz

import com.hazelcast.core.MapStore
import com.walmartlabs.bigben.BigBen.entityProvider
import com.walmartlabs.bigben.entities.Bucket
import com.walmartlabs.bigben.extns.fetch
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.reduce
import com.walmartlabs.bigben.utils.retriable
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit.MINUTES

/**
 * Created by smalik3 on 3/3/18
 */
class BucketStore : MapStore<ZonedDateTime, Bucket> {

    private val l = logger<BucketStore>()

    private val provider = entityProvider<Bucket>()

    override fun deleteAll(keys: MutableCollection<ZonedDateTime>?) {
        throw UnsupportedOperationException("not supported")
    }

    override fun load(key: ZonedDateTime): Bucket? {
        return { fetch<Bucket> { it.bucketId = key } }.retriable("load-bucket: $key").get(1, MINUTES)
    }

    override fun loadAll(keys: Collection<ZonedDateTime>): Map<ZonedDateTime, Bucket> {
        return keys.map { k -> { fetch<Bucket> { it.bucketId = k } }.retriable("load-bucket: $k") }.reduce().get(1, MINUTES).associate { it!!.bucketId!! to it }
    }

    override fun store(key: ZonedDateTime, value: Bucket) {
        if (l.isDebugEnabled) l.debug("saving bucket: {}", key);
        { provider.save(value.apply { bucketId = key }) }.retriable("save-bucket: $key").get(1, MINUTES)
    }

    override fun storeAll(map: Map<ZonedDateTime, Bucket>) {
        if (l.isDebugEnabled) l.debug("saving buckets: {}", map.keys)
        map.entries.map { e -> { provider.save(e.run { value.bucketId = key; value }) }.retriable("save-bucket: ${e.key}") }
    }

    override fun loadAllKeys(): Iterable<ZonedDateTime>? = null

    override fun delete(key: ZonedDateTime) {
        throw UnsupportedOperationException("not supported")
    }
}