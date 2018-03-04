package com.walmartlabs.opensource.bigben.app

/**
 * Created by smalik3 on 2/28/18
 */
import com.walmartlabs.opensource.bigben.api.EventReceiver
import com.walmartlabs.opensource.bigben.api.EventService
import com.walmartlabs.opensource.bigben.core.ScheduleScanner
import com.walmartlabs.opensource.bigben.extns.bucket
import com.walmartlabs.opensource.bigben.extns.json
import com.walmartlabs.opensource.bigben.extns.logger
import com.walmartlabs.opensource.bigben.extns.nowUTC
import com.walmartlabs.opensource.bigben.hz.ClusterSingleton
import com.walmartlabs.opensource.bigben.hz.Hz
import com.walmartlabs.opensource.bigben.providers.domain.cassandra.ClusterConfig


fun main(args: Array<String>) {
    val cc = ClusterConfig()
    cc.contactPoints = "127.0.0.1"
    System.setProperty("bigben.cassandra.config", cc.json())
    System.setProperty("skip.tenant.validation", "yes")
    System.setProperty("bigben.hz.config", mapOf<String, Any>("map" to mapOf("store" to mapOf<String, Any>("writeDelay" to 0))).json())

    val l = logger("app")

    val hz = Hz()
    val service = ScheduleScanner(hz)
    ClusterSingleton(service, hz)
    val eventReceiver = EventReceiver(hz)
    val es = EventService(hz, service, eventReceiver)

    val applicationJson = "application/typeRefJson"

    l.info("starting bigben")

    val current = nowUTC().bucket()
    generateEvents(EventGeneration(current.plusMinutes(1).toString(), 1L, 1, "default"), eventReceiver)
    generateEvents(EventGeneration(current.plusMinutes(2).toString(), 1L, 10000, "default"), eventReceiver)

}

/*path("/events") {
    before("/*") { _, resp -> resp.type(applicationJson) }
    get("/ping", applicationJson) { _, resp ->
        resp.status(200)
        mapOf("status" to "OK").json()
    }
    get("/cluster", applicationJson) { _, _ ->
        es.clusterStats()
    }
    post("/generate", applicationJson) { r, _ ->
        generateEvents(EventGeneration::class.java.fromJson(r.body()), eventReceiver)
    }
}*/