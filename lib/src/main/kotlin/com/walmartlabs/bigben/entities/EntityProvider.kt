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
package com.walmartlabs.bigben.entities

import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.extns.epoch
import java.time.ZonedDateTime

/**
 * Created by smalik3 on 2/25/18
 */
interface EntityProvider<T> {
    fun selector(type: Class<T>): T
    fun raw(selector: T): T
    fun fetch(selector: T): ListenableFuture<T?>
    fun kvs(selector: KV): ListenableFuture<List<KV>>
    fun save(selector: T): ListenableFuture<T>
    fun remove(selector: T): ListenableFuture<T>
    fun unwrap(): Any?
}

interface EventLoader {
    fun load(bucketId: ZonedDateTime, shard: Int, fetchSize: Int, eventTime: ZonedDateTime = epoch(), eventId: String = "", context: Any? = null): ListenableFuture<Pair<Any?, List<Event>>>
}
