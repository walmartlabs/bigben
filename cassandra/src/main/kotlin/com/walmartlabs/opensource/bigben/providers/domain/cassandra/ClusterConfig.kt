package com.walmartlabs.opensource.bigben.providers.domain.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS
import com.datastax.driver.core.SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS

/**
 * Created by smalik3 on 3/2/18
 */
class ClusterConfig {
    var clusterName: String? = null
    var contactPoints: String? = null
    var port = 9042
    var localDataCenter: String? = null

    var compression: String? = null
    var coreConnectionsPerHost = 8
    var maxHostsPerConnection = 32768
    var keepTCPConnectionAlive = true

    var connectionTimeOut = DEFAULT_CONNECT_TIMEOUT_MILLIS
    var readTimeout = DEFAULT_READ_TIMEOUT_MILLIS
    var reconnectPeriod = 5L

    var username: String? = null
    var password: String? = null
    var downgradingConsistency: Boolean = false
}

interface ClusterFactory {
    fun create(): Cluster
}