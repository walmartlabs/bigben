package com.walmartlabs.bigben.app

import com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT
import com.walmartlabs.bigben.BigBen.module
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.cron.CronService
import com.walmartlabs.bigben.extns.APIResponse
import com.walmartlabs.bigben.utils.commons.Props
import com.walmartlabs.bigben.utils.typeRefJson
import io.ktor.application.Application
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.features.ContentNegotiation
import io.ktor.http.HttpStatusCode
import io.ktor.jackson.jackson
import io.ktor.request.receive
import io.ktor.response.header
import io.ktor.response.respond
import io.ktor.routing.*
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import org.apache.log4j.xml.DOMConfigurator
import java.io.File
import java.util.concurrent.CountDownLatch

object AppRun {

    val latch = CountDownLatch(1)

    @JvmStatic
    fun main(args: Array<String>) {
        System.getenv("APP_ROOT")?.run {
            if (File(this, "log4j-overrides.xml").exists()) {
                println("configuring logger")
                DOMConfigurator.configure("log4j-overrides.xml")
            }
        }
        App()
        embeddedServer(Netty, Props.int("app.server.port")) {
            configure()
            routing {
                get("/ping") { call.respond(mapOf("status" to "OK")) }
                route("/events") {
                    val es = module<EventService>()
                    get("/cluster") { call.fromAPIResponse(es.clusterStats()) }
                    post("/schedule") { call.fromAPIResponse(es.schedule(typeRefJson(call.receive()))) }
                    post("/tenant/register") { call.fromAPIResponse(es.registerProcessor(call.receive())) }
                    get("/tenants") { call.fromAPIResponse(es.registeredTenants()) }
                    get("/find") { call.fromAPIResponse(es.find(call.request.queryParameters["id"]!!, call.request.queryParameters["tenant"]!!)) }
                    post("/dryrun") { call.fromAPIResponse(es.dryrun(call.request.queryParameters["id"]!!, call.request.queryParameters["tenant"]!!)) }
                }
                post("/generation/random") { call.respond(EventGenerator.generateEvents(call.receive())) }
                route("/cron") {
                    post { call.fromAPIResponse(CronService.upsert(call.receive())) }
                    get("/describe") { call.fromAPIResponse(CronService.describe(call.receive())) }
                    get("/{tenant}/{id}") {
                        call.fromAPIResponse(
                            CronService.get(
                                call.parameters["tenant"]!!, call.parameters["id"]!!,
                                call.request.queryParameters["describe"]?.toBoolean()
                            )
                        )
                    }
                    delete("/{tenant}/{id}/{type}") {
                        call.fromAPIResponse(CronService.delete(call.parameters["tenant"]!!, call.parameters["id"]!!, call.parameters["type"]!!))
                    }
                }
            }
            latch.countDown()
        }.start(wait = true)
    }
}


private suspend fun ApplicationCall.fromAPIResponse(r: APIResponse) {
    r.headers.forEach { h -> h.value.forEach { response.header(h.key, it) } }
    response.status(HttpStatusCode.fromValue(r.status))
    respond(r.entity)
}

fun Application.configure() {
    install(ContentNegotiation) {
        jackson { enable(INDENT_OUTPUT) }
    }
}