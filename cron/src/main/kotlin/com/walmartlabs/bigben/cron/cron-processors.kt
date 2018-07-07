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

import com.hazelcast.map.AbstractEntryProcessor
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.serialization.DataSerializable
import com.walmartlabs.bigben.extns.utc
import com.walmartlabs.bigben.utils.fromJson
import com.walmartlabs.bigben.utils.json
import com.walmartlabs.bigben.utils.typeRefJson
import java.io.Serializable
import java.time.ZonedDateTime

/**
 * Created by smalik3 on 7/6/18
 */
abstract class DataSerializableEntryProcessor<K, T>(protected var value: String? = null, applyOnBackup: Boolean) : AbstractEntryProcessor<K, T>(applyOnBackup), DataSerializable {
    override fun writeData(out: ObjectDataOutput) = out.run { writeUTF(value) }
    override fun readData(`in`: ObjectDataInput) = `in`.run { value = readUTF() }
}

class CronDeleteEntryProcessor(cronId: String? = null) : DataSerializableEntryProcessor<Int, Crons>(cronId, true), Serializable {
    override fun process(entry: MutableMap.MutableEntry<Int, Crons?>): Any? {
        return entry.setValue(entry.value.apply { this!!.crons.remove(value) }).let { null }
    }
}

class CronEntryProcessor(c: String? = null) : DataSerializableEntryProcessor<Int, Crons>(c, true) {
    override fun process(entry: MutableMap.MutableEntry<Int, Crons?>): Any? {
        val cron = Cron::class.java.fromJson(value!!)
        return entry.setValue(entry.value.apply { CronRunner.crons.values.forEach { this!!.crons[cron.cronId()] = cron } }).let { null }
    }
}

class CronMatchExecutionTimeProcessor(millis: Long? = null) : DataSerializableEntryProcessor<Int, Crons>(millis?.toString(), true) {
    override fun process(entry: MutableMap.MutableEntry<Int, Crons>): List<String> {
        val zdt = utc(value!!.toLong())
        return ArrayList(entry.value.crons.filter { it.value.executionTime().isMatch(zdt) }.values.map { it.json() })
    }
}

class CronUpdateExecutionTimeEntryProcessor(cronId: String? = null, lastExecution: String? = null) : DataSerializableEntryProcessor<Int, Crons>((cronId to lastExecution).json(), true) {
    override fun process(entry: MutableMap.MutableEntry<Int, Crons?>): Any? {
        val (cronId, lastExecution) = typeRefJson<Pair<String, String>>(value!!)
        return entry.setValue(entry.value.apply { this!!.crons[cronId]?.let { it.lastExecutionTime = ZonedDateTime.parse(lastExecution) } }).let { null }
    }
}
