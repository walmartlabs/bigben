package com.walmartlabs.opensource.bigben.providers.domain.cassandra

import com.datastax.driver.core.*
import com.datastax.driver.core.HostDistance.LOCAL
import com.datastax.driver.core.HostDistance.REMOTE
import com.datastax.driver.core.policies.*
import com.datastax.driver.mapping.Mapper
import com.datastax.driver.mapping.Mapper.Option.consistencyLevel
import com.datastax.driver.mapping.Mapper.Option.saveNullFields
import com.datastax.driver.mapping.MappingManager
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.opensource.bigben.entities.*
import com.walmartlabs.opensource.core.fromJson
import com.walmartlabs.opensource.core.logger
import com.walmartlabs.opensource.core.transform
import com.walmartlabs.opensource.core.utils.Props
import java.time.ZonedDateTime

/**
 * Created by smalik3 on 3/2/18
 */
open class CassandraProvider<T : Any> : EntityProvider<T>, ClusterFactory, EventLoader {

    companion object {
        private val l = logger<CassandraProvider<*>>()
        private val cluster: Cluster
        private val mappingManager: MappingManager
        private val loaderQuery: PreparedStatement
        private val session: Session

        private val clusterConfig = ClusterConfig::class.java.fromJson(Props.string("bigben.cassandra.config"))
        private val writeConsistency = consistencyLevel(clusterConfig.writeConsistency)
        private val readConsistency = consistencyLevel(clusterConfig.readConsistency)

        init {
            if (l.isInfoEnabled) l.info("initialing the Cassandra domain provider")
            cluster = (Class.forName(Props.string("bigben.cassandra.cluster.factory", CassandraProvider::class.java.name)).newInstance() as ClusterFactory).create()
            session = cluster.connect()
            mappingManager = MappingManager(session)
            loaderQuery = mappingManager.session.prepare("SELECT * FROM bigben.events WHERE bucket_id = ? AND shard = ? AND (event_time, id) > (?,?) LIMIT ?")
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun selector(type: Class<T>): T {
        return when (type) {
            Event::class.java -> EventC() as T
            Bucket::class.java -> BucketC() as T
            EventLookup::class.java -> EventLookupC() as T
            else -> throw IllegalArgumentException("unknown entity $type")
        }
    }

    override fun raw(selector: T) = selector

    override fun fetch(selector: T): ListenableFuture<T?> {
        if (l.isDebugEnabled) l.debug("fetching ")
        return mappingManager.mapper(selector::class.java).let {
            when (selector) {
                is EventC -> {
                    require(selector.eventTime != null && selector.id != null &&
                            selector.shard != null && selector.shard!! >= 0) { "event keys not provided: $selector" }
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
                else -> throw IllegalArgumentException("unknown selector: $selector")
            }
        }.apply {
            transform { println("fetching entity: $it") }
        }
    }

    override fun save(selector: T): ListenableFuture<T> {
        return mappingManager.mapper(selector::class.java).let {
            @Suppress("UNCHECKED_CAST")
            val m = it as Mapper<Any>
            when (selector) {
                is EventC -> {
                    require(selector.eventTime != null && selector.id != null &&
                            selector.shard != null && selector.shard!! >= 0) { "event keys not provided: $selector" }
                }
                is BucketC -> {
                    require(selector.bucketId != null) { "bucket id not provided: $selector" }
                }
                is EventLookupC -> {
                    require(selector.tenant != null && selector.xrefId != null) { "look up keys not provided: $selector" }
                }
                else -> throw IllegalArgumentException("unknown selector: $selector")
            }
            println("saving entity $selector")
            m.saveAsync(selector, saveNullFields(false), writeConsistency).transform { _ -> selector }
        }
    }

    override fun remove(selector: T): ListenableFuture<T> {
        return mappingManager.mapper(selector::class.java).let {
            @Suppress("UNCHECKED_CAST")
            val m = it as Mapper<Any>
            when (selector) {
                is EventC -> {
                    require(selector.eventTime != null && selector.id != null &&
                            selector.shard != null && selector.shard!! >= 0) { "event keys not provided: $selector" }
                }
                is BucketC -> {
                    require(selector.bucketId != null) { "bucket id not provided: $selector" }
                }
                is EventLookupC -> {
                    require(selector.tenant != null && selector.xrefId != null) { "look up keys not provided: $selector" }
                }
                else -> throw IllegalArgumentException("unknown selector: $selector")
            }
            println("deleting entity $selector")
            m.deleteAsync(selector, writeConsistency).transform { _ -> selector }
        }
    }

    override fun create(): Cluster {
        return Cluster.builder().
                withCodecRegistry(CodecRegistry()
                        .register(EnumCodec(EventStatus.values().toSet()))
                        .register(ZdtCodec()))
                .withClusterName(clusterConfig.clusterName)
                .withPort(clusterConfig.port)
                .also {
                    if (clusterConfig.compression != null)
                        it.withCompression(ProtocolOptions.Compression.valueOf(clusterConfig.compression!!))
                }
                .withRetryPolicy(if (clusterConfig.downgradingConsistency) DowngradingConsistencyRetryPolicy.INSTANCE else DefaultRetryPolicy.INSTANCE)
                .also {
                    if (clusterConfig.localDataCenter != null)
                        it.withLoadBalancingPolicy(TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(clusterConfig.localDataCenter).withUsedHostsPerRemoteDc(0).build()))
                }
                .withReconnectionPolicy(ConstantReconnectionPolicy(clusterConfig.reconnectPeriod))
                .withSocketOptions(SocketOptions().apply {
                    connectTimeoutMillis = clusterConfig.connectionTimeOut
                    readTimeoutMillis = clusterConfig.readTimeout
                    keepAlive = clusterConfig.keepTCPConnectionAlive
                })
                .withPoolingOptions(PoolingOptions().apply {
                    setConnectionsPerHost(LOCAL, clusterConfig.coreConnectionsPerHost, clusterConfig.maxHostsPerConnection)
                    setConnectionsPerHost(REMOTE, 0, 0)
                    heartbeatIntervalSeconds = 60
                }).also { if (clusterConfig.username != null) it.withCredentials(clusterConfig.username, clusterConfig.password) }
                .addContactPoints(*(clusterConfig.contactPoints?.split(",")?.toTypedArray() ?:
                        throw ExceptionInInitializerError("contact points not provided")))
                .build()
    }

    override fun unwrap() = session

    override fun load(bucketId: ZonedDateTime, shard: Int, fetchSize: Int, eventTime: ZonedDateTime, eventId: String, context: Any?): ListenableFuture<Pair<Any?, List<Event>>> {
        return mappingManager.session.executeAsync(loaderQuery.bind(bucketId, shard, eventTime, eventId)).
                transform { null to mappingManager.mapper(EventC::class.java).map(it!!).toList() }
    }
}