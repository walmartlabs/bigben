package com.walmartlabs.bigben.app

import com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT
import com.walmartlabs.bigben.BigBen.module
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.cron.CronService
import com.walmartlabs.bigben.extns.APIResponse
import com.walmartlabs.bigben.utils.stackTraceAsString
import com.walmartlabs.bigben.utils.typeRefJson
import io.ktor.application.Application
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.features.ContentNegotiation
import io.ktor.features.StatusPages
import io.ktor.http.HttpStatusCode
import io.ktor.http.HttpStatusCode.Companion.BadRequest
import io.ktor.http.HttpStatusCode.Companion.InternalServerError
import io.ktor.jackson.jackson
import io.ktor.request.receive
import io.ktor.response.header
import io.ktor.response.respond
import io.ktor.routing.*
import io.ktor.server.netty.EngineMain
import org.apache.commons.text.StrLookup
import org.apache.commons.text.StrSubstitutor
import org.apache.log4j.xml.DOMConfigurator
import java.io.File

fun main(args: Array<String>) = EngineMain.main(args)

fun logs() {
    System.getProperty("bigben.log.config")?.run {
        val logFile = File(this)
        if (logFile.exists()) {
            println("configuring logger")
            StrSubstitutor(StrLookup.systemPropertiesLookup()).run {
                logFile.readLines().map { replace(it) }
            }.joinToString("\n").run {
                File(System.getProperty("java.io.tmpdir"), "log4j-overrides-substituted.xml").let {
                    println("using log file from ${it.absolutePath}")
                    it.writeText(this)
                    DOMConfigurator.configure(it.toURI().toURL())
                }
            }
        }
    }
}

fun app() = App()

fun Application.routes() {
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
}

fun Application.configure() {
    install(ContentNegotiation) {
        jackson { enable(INDENT_OUTPUT) }
    }
    install(StatusPages) {
        exception<IllegalArgumentException> { e ->
            call.response.status(BadRequest)
            call.respond(mapOf("message" to (e.message ?: "")))
        }
        exception<Throwable> { e ->
            call.response.status(InternalServerError)
            if (call.request.queryParameters["debug"] != null) {
                call.respond(mapOf("message" to ((e.message ?: "")), "stacktrace" to e.stackTraceAsString()))
            } else call.respond(mapOf("message" to (e.message ?: "")))
        }
    }
}

private suspend fun ApplicationCall.fromAPIResponse(r: APIResponse) {
    r.headers.forEach { h -> h.value.forEach { response.header(h.key, it) } }
    response.status(HttpStatusCode.fromValue(r.status))
    respond(r.entity)
}