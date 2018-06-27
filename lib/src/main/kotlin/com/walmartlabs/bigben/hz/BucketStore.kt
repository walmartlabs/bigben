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
