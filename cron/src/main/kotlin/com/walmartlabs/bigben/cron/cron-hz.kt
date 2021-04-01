/*-
 * #%L
 * Bigben:cron
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
package com.walmartlabs.bigben.cron

import com.hazelcast.core.MapStore
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.serialization.DataSerializable
import com.walmartlabs.bigben.entities.KV
import com.walmartlabs.bigben.extns.kvs
import com.walmartlabs.bigben.extns.save
import com.walmartlabs.bigben.utils.*
import com.walmartlabs.bigben.utils.commons.Props.int

/**
 * Created by smalik3 on 7/3/18
 */
data class Crons @JvmOverloads constructor(var crons: MutableMap<String, Cron> = HashMap()) : DataSerializable {
    override fun writeData(out: ObjectDataOutput) = out.run { writeInt(crons.size); crons.forEach { writeUTF(it.value.json()) } }
    override fun readData(ins: ObjectDataInput) = ins.run { (1..readInt()).forEach { Cron::class.java.fromJson(readUTF()).apply { crons[fqdnCronId()] = this } } }
}

class CronMapStore : MapStore<Int, Crons> {

    private val l = logger<CronMapStore>()

    override fun storeAll(map: Map<Int, Crons>) {
        { map.entries.map { e -> save<KV> { it.key = e.key.toString(); it.column = ""; it.value = e.value.yaml() } }.reduce() }
                .retriable("cron-map-store:store-all").result { l.error("error in storing / updating crons for keys: ${map.keys}", it.rootCause()!!); throw it.rootCause()!! }
    }

    override fun store(key: Int, value: Crons) = storeAll(mapOf(key to value))

    override fun loadAllKeys(): Iterable<Int> = (1..int("cron.partitions.count", 271)).toList()

    override fun loadAll(keys: Collection<Int>): Map<Int, Crons> {
        if (l.isInfoEnabled) l.info("bulk-loading cron keys: $keys, thread: ${Thread.currentThread().name}")
        return { keys.map { k -> kvs { it.key = k.toString(); it.column = "" }.transform { k to it!! }.catching { println(it!!.stackTrace); 0 to emptyList() } }.reduce() }
                .retriable("cron-map-store:load-all")
                .result { l.error("error in loading crons for keys: $keys", it.rootCause()); throw it.rootCause()!! }
                .associate {
                    if (it.second.isEmpty()) it.first to Crons()
                    else it.second[0].key!!.toInt() to typeRefYaml(it.second[0].value!!)
                }.apply { if (l.isInfoEnabled) this.filter { it.value.crons.isNotEmpty() }.apply { if (this.isNotEmpty()) l.info("crons loaded: $this}") } }
    }

    override fun deleteAll(keys: Collection<Int>) = throw UnsupportedOperationException("this must never have happened, keys: $keys")

    override fun load(key: Int) = loadAll(listOf(key))[key]

    override fun delete(key: Int) = throw UnsupportedOperationException("this must never have happened, key: $key")
}
