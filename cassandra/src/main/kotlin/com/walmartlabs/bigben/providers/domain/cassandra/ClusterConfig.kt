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
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE
import com.datastax.driver.core.SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS
import com.datastax.driver.core.SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS

/**
 * Created by smalik3 on 3/2/18
 */
data class ClusterConfig(
    val clusterName: String = "bigben",
    val contactPoints: String,
    val port: Int = 9042,
    val localDataCenter: String?,

    val compression: String?,
    val keepTCPConnectionAlive: Boolean = true,

    val coreConnectionsPerLocalHost: Int = 1,
    val maxConnectionsPerLocalHost: Int = 1,
    val coreConnectionsPerRemoteHost: Int = 1,
    val maxConnectionsPerRemoteHost: Int = 1,
    val maxRequestsPerLocalConnection: Int = 32768,
    val maxRequestsPerRemoteConnection: Int = 2048,
    val newLocalConnectionThreshold: Int = 3000,
    val newRemoteConnectionThreshold: Int = 400,
    val poolTimeoutMillis: Int = 0,

    val connectionTimeOut: Int = DEFAULT_CONNECT_TIMEOUT_MILLIS,
    val readTimeout: Int = DEFAULT_READ_TIMEOUT_MILLIS,
    val reconnectPeriod: Long = 5L,

    val username: String?,
    val password: String?,
    val downgradingConsistency: Boolean = false,

    val writeConsistency: ConsistencyLevel = LOCAL_ONE,
    val readConsistency: ConsistencyLevel = LOCAL_ONE
)

interface ClusterFactory {
    fun create(): Cluster
}
