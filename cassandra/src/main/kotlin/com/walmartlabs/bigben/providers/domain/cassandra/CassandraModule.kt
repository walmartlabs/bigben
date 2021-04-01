/*-
 * #%L
 * BigBen:cassandra
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
package com.walmartlabs.bigben.providers.domain.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.CodecRegistry
import com.datastax.driver.core.HostDistance.LOCAL
import com.datastax.driver.core.HostDistance.REMOTE
import com.datastax.driver.core.PoolingOptions
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ProtocolOptions
import com.datastax.driver.core.Session
import com.datastax.driver.core.SocketOptions
import com.datastax.driver.core.policies.ConstantReconnectionPolicy
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
import com.datastax.driver.core.policies.DefaultRetryPolicy
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy
import com.datastax.driver.core.policies.TokenAwarePolicy
import com.datastax.driver.mapping.Mapper
import com.datastax.driver.mapping.Mapper.Option.consistencyLevel
import com.datastax.driver.mapping.Mapper.Option.saveNullFields
import com.datastax.driver.mapping.Mapper.Option.ttl
import com.datastax.driver.mapping.MappingManager
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.bigben.entities.Bucket
import com.walmartlabs.bigben.entities.EntityProvider
import com.walmartlabs.bigben.entities.Event
import com.walmartlabs.bigben.entities.EventLoader
import com.walmartlabs.bigben.entities.EventLookup
import com.walmartlabs.bigben.entities.EventStatus
import com.walmartlabs.bigben.entities.KV
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.utils.commons.Module
import com.walmartlabs.bigben.utils.commons.ModuleRegistry
import com.walmartlabs.bigben.utils.commons.Props.map
import com.walmartlabs.bigben.utils.commons.Props.string
import com.walmartlabs.bigben.utils.fromJson
import com.walmartlabs.bigben.utils.json
import com.walmartlabs.bigben.utils.logger
import com.walmartlabs.bigben.utils.transform
import java.time.ZonedDateTime

/**
 * Created by smalik3 on 3/2/18
 */
open class CassandraModule<T : Any> : EntityProvider<T>, ClusterFactory, EventLoader, Module {

    companion object {
        private val l = logger<CassandraModule<*>>()
        private val cluster: Cluster
        val mappingManager: MappingManager
        private val loaderQuery: PreparedStatement
        private val kvAllQuery: PreparedStatement
        private val session: Session

        private val clusterConfig = ClusterConfig::class.java.fromJson(map("cassandra.cluster").json())
        private val writeConsistency = clusterConfig.writeConsistency
        private val readConsistency = clusterConfig.readConsistency

        init {
            l.info("initialing the Cassandra module")
            cluster = (Class.forName(string("domain.cluster.factory.class", CassandraModule::class.java.name)).newInstance() as ClusterFactory).create()
            session = cluster.connect(string("cassandra.keyspace"))
            mappingManager = MappingManager(session)
            loaderQuery = mappingManager.session.prepare("SELECT * FROM ${session.loggedKeyspace}.events WHERE bucket_id = ? AND shard = ? AND (event_time, id) > (?,?) LIMIT ?;")
            kvAllQuery = mappingManager.session.prepare("SELECT * FROM ${session.loggedKeyspace}.kv_table WHERE key = ?;")
        }
    }

    override fun init(registry: ModuleRegistry) {
    }

    @Suppress("UNCHECKED_CAST")
    override fun selector(type: Class<T>): T {
        return when (type) {
            Event::class.java -> EventC() as T
            Bucket::class.java -> BucketC() as T
            EventLookup::class.java -> EventLookupC() as T
            KV::class.java -> KVC() as T
            else -> type.newInstance()
        }
    }

    override fun raw(selector: T) = selector

    override fun kvs(selector: KV): ListenableFuture<List<KV>> {
        require(selector.key != null) { "key must be provided" }
        return session.executeAsync(kvAllQuery.bind(selector.key)).transform {
            it?.run { mappingManager.mapper(KVC::class.java).map(this).map { it } } ?: emptyList()
        }
    }

    override fun fetch(selector: T): ListenableFuture<T?> {
        val readC = (selector as? ConsistencyOverride)?.read()?.let { it } ?: readConsistency
        return mappingManager.mapper(selector::class.java).let {
            val readConsistency = consistencyLevel(readC)
            when (selector) {
                is EventC -> {
                    require(
                            selector.eventTime != null && selector.id != null &&
                                    selector.shard != null && selector.shard!! >= 0
                    ) { "event keys not provided: $selector" }
                    it.getAsync(selector.bucketId, selector.shard, selector.eventTime, selector.id, readConsistency).transform { it }
                }
                is BucketC -> {
                    require(selector.bucketId != null) { "bucket id not provided: $selector" }
                    it.getAsync(selector.bucketId, readConsistency).transform { it }
                }
                is EventLookupC -> {
                    require(selector.tenant != null && selector.xrefId != null) { "look up keys not provided: $selector" }
                    it.getAsync(selector.tenant, selector.xrefId, readConsistency).transform { it }
                }
                is KVC -> {
                    require(selector.key != null && selector.column != null) { "kv keys not provided: $selector" }
                    it.getAsync(selector.key, selector.column, readConsistency).transform { it }
                }
                else -> throw IllegalArgumentException("unknown selector: $selector")
            }
        }.apply {
            transform { if (l.isDebugEnabled) l.debug("fetched entity: {}, readConsistency: {}", it, readC) }
        }
    }

    override fun save(selector: T): ListenableFuture<T> {
        return mappingManager.mapper(selector::class.java).let {
            @Suppress("UNCHECKED_CAST")
            val m = it as Mapper<Any>
            when (selector) {
                is EventC -> {
                    require(
                            selector.eventTime != null && selector.id != null && selector.bucketId != null &&
                                    selector.shard != null && selector.shard!! >= 0
                    ) { "event keys not provided: $selector" }
                }
                is BucketC -> {
                    require(selector.bucketId != null) { "bucket id not provided: $selector" }
                }
                is EventLookupC -> {
                    require(selector.tenant != null && selector.xrefId != null) { "look up keys not provided: $selector" }
                    selector.lastModified = nowUTC()
                }
                is KVC -> {
                    require(selector.key != null && selector.column != null) { "kv keys not provided: $selector" }
                    selector.lastModified = nowUTC()
                }
            }
            val writeConsistency = (selector as? ConsistencyOverride)?.write()?.let { it } ?: writeConsistency
            val ttl = (selector as? TTLOverride)?.ttl()?.let { it } ?: 0

            if (l.isDebugEnabled) l.debug("saving entity {}, ttl: {}, writeConsistency: {}", selector, ttl, writeConsistency)
            m.saveAsync(selector, saveNullFields(false), consistencyLevel(writeConsistency), ttl(ttl))
                    .transform { if (l.isDebugEnabled) l.debug("saved entity {}", selector); selector }
        }
    }

    override fun remove(selector: T): ListenableFuture<T> {
        return mappingManager.mapper(selector::class.java).let {
            @Suppress("UNCHECKED_CAST")
            val m = it as Mapper<Any>
            when (selector) {
                is EventC -> {
                    require(
                            selector.eventTime != null && selector.id != null &&
                                    selector.shard != null && selector.shard!! >= 0
                    ) { "event keys not provided: $selector" }
                }
                is BucketC -> {
                    require(selector.bucketId != null) { "bucket id not provided: $selector" }
                }
                is EventLookupC -> {
                    require(selector.tenant != null && selector.xrefId != null) { "look up keys not provided: $selector" }
                }
                is KVC -> {
                    require(selector.key != null && selector.column != null) { "kv keys not provided: $selector" }
                }
            }
            val deleteConsistency = (selector as? ConsistencyOverride)?.delete()?.let { it } ?: writeConsistency
            if (l.isDebugEnabled) l.debug("deleting entity: {}, deleteConsistency: $deleteConsistency", selector)
            m.deleteAsync(selector, consistencyLevel(deleteConsistency)).transform { if (l.isDebugEnabled) l.debug("deleted entity {}", selector); selector }
        }
    }

    override fun create(): Cluster {
        return Cluster.builder()
                .withCodecRegistry(CodecRegistry().register(EnumCodec(EventStatus.values().toSet())).register(ZdtCodec()))
                .withClusterName(clusterConfig.clusterName)
                .withPort(clusterConfig.port)
                .also { clusterConfig.compression?.run { it.withCompression(ProtocolOptions.Compression.valueOf(this)) } }
                .withRetryPolicy(if (clusterConfig.downgradingConsistency) DowngradingConsistencyRetryPolicy.INSTANCE else DefaultRetryPolicy.INSTANCE)
                .also {
                    clusterConfig.localDataCenter?.run {
                        it.withLoadBalancingPolicy(TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(this).withUsedHostsPerRemoteDc(0).build()))
                    }
                }
                .withReconnectionPolicy(ConstantReconnectionPolicy(clusterConfig.reconnectPeriod))
                .withSocketOptions(SocketOptions().apply {
                    connectTimeoutMillis = clusterConfig.connectionTimeOut
                    readTimeoutMillis = clusterConfig.readTimeout
                    keepAlive = clusterConfig.keepTCPConnectionAlive
                })
                .withPoolingOptions(PoolingOptions().apply {
                    clusterConfig.apply {
                        setConnectionsPerHost(LOCAL, coreConnectionsPerLocalHost, maxConnectionsPerLocalHost)
                        setConnectionsPerHost(REMOTE, coreConnectionsPerRemoteHost, maxConnectionsPerRemoteHost)
                    }
                    heartbeatIntervalSeconds = 60
                })
                .also { clusterConfig.username?.run { it.withCredentials(this, clusterConfig.password) } }
                .addContactPoints(*clusterConfig.contactPoints.split(",").toTypedArray())
                .apply { decorate(this) }
                .build()
    }

    protected open fun decorate(builder: Cluster.Builder) {
    }

    override fun unwrap() = session

    override fun load(bucketId: ZonedDateTime, shard: Int, fetchSize: Int, eventTime: ZonedDateTime, eventId: String, context: Any?): ListenableFuture<Pair<Any?, List<Event>>> {
        return mappingManager.session.executeAsync(loaderQuery.bind(bucketId, shard, eventTime, eventId, fetchSize)).transform { null to mappingManager.mapper(EventC::class.java).map(it!!).toList() }
    }
}
