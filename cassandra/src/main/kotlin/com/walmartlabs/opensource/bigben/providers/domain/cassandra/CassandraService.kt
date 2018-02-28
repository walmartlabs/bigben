package com.walmartlabs.opensource.bigben.providers.domain.cassandra

import com.google.auto.service.AutoService
import com.google.common.util.concurrent.ListenableFuture
import com.walmartlabs.opensource.bigben.entities.Entity
import com.walmartlabs.opensource.bigben.entities.Event
import com.walmartlabs.opensource.bigben.entities.EntityProvider
import com.walmartlabs.opensource.bigben.tasks.EventLoader

/**
 * Created by smalik3 on 2/25/18
 */
@AutoService(EntityProvider::class)
class CassandraService<K, T : Entity<K>> : EntityProvider<K, T> {

    override fun loader(): EventLoader<T> {
    }

    override fun selector(type: Class<T>): T {
    }

    override fun fetch(selector: T): ListenableFuture<T?> {
        when
    }

    override fun save(selector: T): ListenableFuture<T> {
    }
}