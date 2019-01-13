package com.walmartlabs.bigben.app

import com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT
import com.walmartlabs.bigben.BigBen.module
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.cron.CronService
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
import javax.ws.rs.core.Response

fun main(args: Array<String>) {
    embeddedServer(Netty, 8080) {
        configure()
        App()
        routing {
            get("/ping") { call.respond(mapOf("status" to "OK")) }
            route("/events") {
                val es = module<EventService>()
                get("/cluster") { call.respondFromJaxrs(es.clusterStats()) }
                post("/schedule") { call.respondFromJaxrs(es.schedule(typeRefJson(call.receive()))) }
                post("/tenant/register") { call.respondFromJaxrs(es.registerProcessor(call.receive())) }
                get("/tenants") { call.respondFromJaxrs(es.registeredTenants()) }
                get("/find") { call.respondFromJaxrs(es.find(call.request.queryParameters["id"]!!, call.request.queryParameters["tenant"]!!)) }
                post("/dryrun") { call.respondFromJaxrs(es.dryrun(call.request.queryParameters["id"]!!, call.request.queryParameters["tenant"]!!)) }
            }
            post("/generation/random") { call.respond(EventGenerator.generateEvents(call.receive())) }
            route("/cron") {
                post { call.respondFromJaxrs(CronService.upsert(call.receive())) }
                get("/describe") { call.respondFromJaxrs(CronService.describe(call.receive())) }
                get("/{tenant}/{id}") {
                    call.respondFromJaxrs(
                        CronService.get(
                            call.parameters["tenant"]!!, call.parameters["id"]!!,
                            call.request.queryParameters["describe"]?.toBoolean()
                        )
                    )
                }
                delete("/{tenant}/{id}/{type}") {
                    call.respondFromJaxrs(CronService.delete(call.parameters["tenant"]!!, call.parameters["id"]!!, call.parameters["type"]!!))
                }
            }
        }
    }.start(wait = true)
}

private suspend fun ApplicationCall.respondFromJaxrs(r: Response) {
    r.headers.forEach { h -> h.value.forEach { response.header(h.key, it.toString()) } }
    response.status(HttpStatusCode.fromValue(r.status))
    respond(r.entity)
}

fun Application.configure() {
    install(ContentNegotiation) {
        jackson { enable(INDENT_OUTPUT) }
    }
}