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
import com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM
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

    var writeConsistency: ConsistencyLevel = LOCAL_QUORUM
    var readConsistency: ConsistencyLevel = LOCAL_QUORUM
}

interface ClusterFactory {
    fun create(): Cluster
}
