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
package com.walmartlabs.bigben.extns

import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.BigBen.entityProvider
import com.walmartlabs.bigben.entities.KV

inline fun <reified T> fetch(selector: (T) -> Unit): ListenableFuture<T?> {
    return entityProvider<T>().let { it.fetch(it.selector(T::class.java).apply { selector(this) }) }
}

inline fun kvs(selector: (KV) -> Unit): ListenableFuture<List<KV>> {
    return entityProvider<KV>().let { it.kvs(it.selector(KV::class.java).apply { selector(this) }) }
}

inline fun <reified T> save(selector: (T) -> Unit): ListenableFuture<T> {
    return entityProvider<T>().let { it.save(it.selector(T::class.java).apply { selector(this) }) }
}

inline fun <reified T> remove(selector: (T) -> Unit): ListenableFuture<T> {
    return entityProvider<T>().let { it.remove(it.selector(T::class.java).apply { selector(this) }) }
}
