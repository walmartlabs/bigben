package com.walmartlabs.bigben.tests

import com.datastax.driver.core.Session
import com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT
import com.walmartlabs.bigben.BigBen
import com.walmartlabs.bigben.BigBen.module
import com.walmartlabs.bigben.app.EventGenerator
import com.walmartlabs.bigben.app.main
import com.walmartlabs.bigben.entities.EntityProvider
import com.walmartlabs.bigben.entities.EventDeliveryOption.FULL_EVENT
import com.walmartlabs.bigben.entities.EventDeliveryOption.PAYLOAD_ONLY
import com.walmartlabs.bigben.entities.EventLoader
import com.walmartlabs.bigben.entities.EventRequest
import com.walmartlabs.bigben.entities.EventResponse
import com.walmartlabs.bigben.entities.EventStatus.*
import com.walmartlabs.bigben.extns.nowUTC
import com.walmartlabs.bigben.kafka.MockMessageProducerFactory
import com.walmartlabs.bigben.processors.ProcessorConfig
import com.walmartlabs.bigben.processors.ProcessorConfig.Type.*
import com.walmartlabs.bigben.utils.fromJson
import com.walmartlabs.bigben.utils.json
import com.walmartlabs.bigben.utils.stackTraceAsString
import com.walmartlabs.bigben.utils.typeRefJson
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.client.HttpClient
import io.ktor.client.call.call
import io.ktor.client.engine.apache.Apache
import io.ktor.client.request.accept
import io.ktor.client.request.post
import io.ktor.client.request.url
import io.ktor.client.response.readText
import io.ktor.content.TextContent
import io.ktor.features.ContentNegotiation
import io.ktor.features.StatusPages
import io.ktor.http.ContentType.Application.Json
import io.ktor.http.HttpMethod
import io.ktor.http.HttpMethod.Companion.Get
import io.ktor.http.HttpMethod.Companion.Post
import io.ktor.http.HttpStatusCode
import io.ktor.http.HttpStatusCode.Companion.OK
import io.ktor.jackson.jackson
import io.ktor.request.contentType
import io.ktor.request.header
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.routing.post
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import java.time.ZonedDateTime
import java.util.UUID.randomUUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.SynchronousQueue
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class APITests {

    companion object {
        init {
            System.setProperty("bigben.configs", "file://bigben-api-test.yaml")
            thread { main(emptyArray()) }
            BigBen.init()
        }
    }

    private val client = HttpClient(Apache)
    private val server = "http://localhost:8080"

    private val responses = ConcurrentHashMap<String, SynchronousQueue<EventResponse>>()
    private val payloadResponse = SynchronousQueue<String>()

    @BeforeClass
    private fun `clean up db`() {
        println("cleaning up the db")
        (module<EntityProvider<Any>>().unwrap() as Session).apply {
            execute("truncate bigben.events;")
            execute("truncate bigben.lookups;")
            execute("truncate bigben.buckets;")
            execute("truncate bigben.kv_table;")
        }
        thread {
            embeddedServer(Netty, 9090) {
                println("starting test server")
                install(ContentNegotiation) {
                    jackson { enable(INDENT_OUTPUT) }
                }
                install(StatusPages) {
                    exception<Throwable> { e ->
                        //e.printStackTrace()
                        call.response.status(HttpStatusCode.InternalServerError)
                        call.respond(mapOf("message" to ((e.message ?: "")), "stacktrace" to e.stackTraceAsString()))
                    }
                }
                routing {
                    post("/test") {
                        if (call.request.queryParameters["error"] != null) {
                            throw IllegalArgumentException("test error")
                        }
                        val er = call.receive<EventResponse>()
                        assertEquals(call.request.header("header1"), "value1")
                        assertEquals(call.request.header("header2"), "value2")
                        assertEquals(call.request.contentType().contentType, Json.contentType)
                        assertEquals(call.request.contentType().contentSubtype, Json.contentSubtype)
                        responses[er.id!!]!!.put(er)
                        call.respond(OK, mapOf("status" to "OK"))
                    }
                    post("/payload") {
                        if (call.request.queryParameters["error"] != null) {
                            throw IllegalArgumentException("test error")
                        }
                        val payload = call.receive<String>()
                        assertEquals(call.request.header("header1"), "value1")
                        assertEquals(call.request.header("header2"), "value2")
                        assertEquals(call.request.contentType().contentType, Json.contentType)
                        assertEquals(call.request.contentType().contentSubtype, Json.contentSubtype)
                        payloadResponse.put(payload)
                        call.respond(OK, mapOf("status" to "OK"))
                    }
                }
            }.start(true)
        }
    }

    @AfterClass
    fun teardown() {
        client.close()
    }

    @Test(enabled = true)
    fun `test events at the same time`() {
        val tenant = "test"

        assertEquals(runBlocking {
            client.call {
                url("$server/events/tenant/register")
                accept(Json)
                method = Post
                body = TextContent(
                    ProcessorConfig(
                        tenant, CUSTOM_CLASS,
                        mapOf("eventProcessorClass" to "com.walmartlabs.bigben.processors.NoOpCustomClassProcessor")
                    ).json(), Json
                )
            }.response.status.value
        }, 200)

        // schedule 1000 events at exactly same time at the start of the minute:
        runBlocking {
            client.post<String> {
                url("$server/generation/random")
                accept(Json)
                body = TextContent(EventGenerator.EventGeneration("PT1M", "PT0S", 1000, tenant).json(), Json)
            }
        }

        // schedule 1000 events at exactly same time at the start of the minute + 30 seconds:
        val bucket = runBlocking {
            client.post<String> {
                url("$server/generation/random")
                accept(Json)
                body = TextContent(EventGenerator.EventGeneration("PT1M30S", "PT0S", 1000, tenant).json(), Json)
            }
        }.run { typeRefJson<Map<String, Int>>(this).run { ZonedDateTime.parse(entries.first().key) } }

        Thread.sleep(2 * 60 * 1000) // sleep for 2 minutes

        var total = 0
        (0..1).forEach {
            // 2000 events -> 2 shards
            var l = module<EventLoader>().load(bucket, 0, 400).get()
            while (l.second.isNotEmpty()) {
                l.second.forEach {
                    assertEquals(it.bucketId, bucket)
                    assertEquals(it.status, PROCESSED)
                    assertTrue { it.eventTime == bucket || it.eventTime == bucket.plusSeconds(30) }
                    total++
                }
                l = module<EventLoader>().load(bucket, 0, 400, l.second.last().eventTime!!, l.second.last().id!!, l.first)
                    .get()
            }
        }
    }

    @Test
    fun `test unknown tenant rejection`() {
        val (status, content) = client.call("/events/schedule", listOf(EventRequest("id123", nowUTC().toString(), "ABC")))
        assertEquals(status, 400)
        assertEquals(typeRefJson<List<EventResponse>>(content)[0].eventStatus, REJECTED)
    }

    @Test
    fun `test missing tenant`() {
        val (status, content) = client.call("/events/schedule", listOf(EventRequest("id123", nowUTC().toString())))
        assertEquals(status, 400)
        assertEquals(typeRefJson<List<EventResponse>>(content)[0].eventStatus, REJECTED)
    }

    @Test
    fun `test missing event time`() {
        val (status, content) = client.call("/events/schedule", listOf(EventRequest("id123", tenant = "ABC")))
        assertEquals(status, 400)
        assertEquals(typeRefJson<List<EventResponse>>(content)[0].eventStatus, REJECTED)
    }

    @Test
    fun `test event time wrong format`() {
        val (status, content) = client.call("/events/schedule", listOf(EventRequest("id123", "time", tenant = "ABC")))
        assertEquals(status, 400)
        assertEquals(typeRefJson<List<EventResponse>>(content)[0].eventStatus, REJECTED)
    }

    @Test
    fun `test event time in past`() {
        val tenant = "http"
        assertEquals(
            client.call(
                "/events/tenant/register", ProcessorConfig(
                    tenant, HTTP, mapOf(
                        "url" to "http://localhost:9090/test", "headers" to mapOf("header1" to "value1", "header2" to "value2")
                    )
                )
            ).first, 200
        )

        val eventId = "id123"
        responses[eventId] = SynchronousQueue()
        val (status, content) = client.call("/events/schedule", listOf(EventRequest(eventId, nowUTC().minusMinutes(1).toString(), tenant)))
        assertEquals(status, 200)
        assertEquals(typeRefJson<List<EventResponse>>(content)[0].eventStatus, TRIGGERED)
        val er = responses[eventId]!!.take()
        assertEquals(er.eventStatus, TRIGGERED)
        assertEquals(er.deliveryOption, FULL_EVENT)
        assertEquals(er.tenant!!, tenant)
    }

    @Test
    fun `test event time in past - payload only`() {
        val tenant = "http3"
        assertEquals(
            client.call(
                "/events/tenant/register", ProcessorConfig(
                    tenant, HTTP, mapOf(
                        "url" to "http://localhost:9090/payload", "headers" to mapOf("header1" to "value1", "header2" to "value2")
                    )
                )
            ).first, 200
        )

        val eventId = "id234"
        responses[eventId] = SynchronousQueue()
        val (status, content) = client.call("/events/schedule", listOf(EventRequest(eventId, nowUTC().minusMinutes(1).toString(), tenant, payload = "testP", deliveryOption = PAYLOAD_ONLY)))
        assertEquals(status, 200)
        assertEquals(typeRefJson<List<EventResponse>>(content)[0].eventStatus, TRIGGERED)
        val payload = payloadResponse.take()
        assertEquals(payload, "testP")
    }

    @Test
    fun `test event - null payload with payload only option`() {
        val tenant = "http2"
        assertEquals(
            client.call(
                "/events/tenant/register", ProcessorConfig(
                    tenant, HTTP, mapOf(
                        "url" to "http://localhost:9090/test", "headers" to mapOf("header1" to "value1", "header2" to "value2")
                    )
                )
            ).first, 200
        )
        val eventId = randomUUID().toString()
        responses[eventId] = SynchronousQueue()
        val (status, content) = client.call("/events/schedule", listOf(EventRequest(eventId, nowUTC().plusMinutes(1).toString(), tenant, deliveryOption = PAYLOAD_ONLY)))
        assertEquals(status, 400)
        assertEquals(typeRefJson<List<EventResponse>>(content)[0].eventStatus, REJECTED)
    }

    @Test
    fun `test find and dryrun APIs`() {
        val tenant = "http4"
        assertEquals(
            client.call(
                "/events/tenant/register", ProcessorConfig(
                    tenant, HTTP, mapOf(
                        "url" to "http://localhost:9090/test", "headers" to mapOf("header1" to "value1", "header2" to "value2")
                    )
                )
            ).first, 200
        )
        val eventId = randomUUID().toString()
        responses[eventId] = SynchronousQueue()
        val eventTime = nowUTC().plusMinutes(100000).toString()
        val (status, content) = client.call("/events/schedule", listOf(EventRequest(eventId, eventTime, tenant, payload = "P1")))
        assertEquals(status, 200)
        assertEquals(typeRefJson<List<EventResponse>>(content)[0].eventStatus, ACCEPTED)

        val (s, c) = client.call("/events/find?tenant=$tenant&id=$eventId", null, Get)
        assertEquals(s, 200)
        val er = EventResponse::class.java.fromJson(c)
        assertEquals(er.eventStatus, UN_PROCESSED)
        assertEquals(er.payload, "P1")
        assertEquals(er.tenant, tenant)
        assertFalse { er.eventId!!.startsWith("a-") }
        assertEquals(er.deliveryOption, FULL_EVENT)
        assertEquals(er.eventTime, eventTime)

        val (s1, c1) = client.call("/events/dryrun?tenant=$tenant&id=$eventId", null)
        assertEquals(s1, 200)
        val er1 = EventResponse::class.java.fromJson(c1)
        assertEquals(er1.eventStatus, UN_PROCESSED)
        assertEquals(er1.payload, "P1")
        assertEquals(er1.tenant, tenant)
        assertFalse { er1.eventId!!.startsWith("a-") }
        assertEquals(er1.deliveryOption, FULL_EVENT)
        assertEquals(er1.eventTime, eventTime)

        responses[eventId]!!.take().apply {
            assertEquals(eventStatus, TRIGGERED)
            assertEquals(payload, "P1")
            assertEquals(this.tenant, tenant)
            assertFalse { this.eventId!!.startsWith("a-") }
            assertEquals(deliveryOption, FULL_EVENT)
            assertEquals(eventTime, eventTime)
        }
    }

    @Test
    fun `test find and dryrun APIs - payload only`() {
        val tenant = "http5"
        assertEquals(
            client.call(
                "/events/tenant/register", ProcessorConfig(
                    tenant, HTTP, mapOf(
                        "url" to "http://localhost:9090/payload", "headers" to mapOf("header1" to "value1", "header2" to "value2")
                    )
                )
            ).first, 200
        )
        val eventId = randomUUID().toString()
        responses[eventId] = SynchronousQueue()
        val eventTime = nowUTC().plusMinutes(100000).toString()
        val (status, content) = client.call("/events/schedule", listOf(EventRequest(eventId, eventTime, tenant, payload = "P1", deliveryOption = PAYLOAD_ONLY)))
        assertEquals(status, 200)
        assertEquals(typeRefJson<List<EventResponse>>(content)[0].eventStatus, ACCEPTED)

        val (s, c) = client.call("/events/find?tenant=$tenant&id=$eventId", null, Get)
        assertEquals(s, 200)
        val er = EventResponse::class.java.fromJson(c)
        assertEquals(er.eventStatus, UN_PROCESSED)
        assertEquals(er.payload, "P1")
        assertEquals(er.tenant, tenant)
        assertTrue { er.eventId!!.startsWith("a-") }
        assertEquals(er.deliveryOption, PAYLOAD_ONLY)
        assertEquals(er.eventTime, eventTime)

        val (s1, c1) = client.call("/events/dryrun?tenant=$tenant&id=$eventId", null)
        assertEquals(s1, 200)
        val er1 = EventResponse::class.java.fromJson(c1)
        assertEquals(er1.eventStatus, UN_PROCESSED)
        assertEquals(er1.payload, "P1")
        assertEquals(er1.tenant, tenant)
        assertTrue { er1.eventId!!.startsWith("a-") }
        assertEquals(er1.deliveryOption, PAYLOAD_ONLY)
        assertEquals(er1.eventTime, eventTime)

        payloadResponse.take().apply {
            assertEquals(this, "P1")
        }
    }

    @Test
    fun `test event kafka tenant API`() {
        val tenant = "kafka1"
        assertEquals(
            client.call(
                "/events/tenant/register", ProcessorConfig(
                    tenant, MESSAGING, mapOf(
                        "topic" to "topic1",
                        "bootstrap.servers" to "localhost:9092"
                    )
                )
            ).first, 200
        )
        val eventId = randomUUID().toString()
        responses[eventId] = SynchronousQueue()
        val eventTime = nowUTC().minusMinutes(1).toString()
        val (status, content) = client.call("/events/schedule", listOf(EventRequest(eventId, eventTime, tenant, payload = "P1")))
        assertEquals(status, 200)
        assertEquals(typeRefJson<List<EventResponse>>(content)[0].eventStatus, TRIGGERED)
        Thread.sleep(2000)
        val er = MockMessageProducerFactory.LAST_MESSAGE.get()
        assertEquals(er.eventStatus, TRIGGERED)
        assertEquals(er.payload, "P1")
        assertEquals(er.tenant, tenant)
        assertEquals(er.deliveryOption, FULL_EVENT)
        assertEquals(er.eventTime, eventTime)
    }

    @Test
    fun `test event kafka tenant API - payload only`() {
        val tenant = "kafka2"
        assertEquals(
            client.call(
                "/events/tenant/register", ProcessorConfig(
                    tenant, MESSAGING, mapOf(
                        "topic" to "topic2",
                        "bootstrap.servers" to "localhost:9092"
                    )
                )
            ).first, 200
        )
        val eventId = randomUUID().toString()
        responses[eventId] = SynchronousQueue()
        val eventTime = nowUTC().minusMinutes(1).toString()
        val (status, content) = client.call("/events/schedule", listOf(EventRequest(eventId, eventTime, tenant, payload = "P2", deliveryOption = PAYLOAD_ONLY)))
        assertEquals(status, 200)
        assertEquals(typeRefJson<List<EventResponse>>(content)[0].eventStatus, TRIGGERED)
        val er = MockMessageProducerFactory.LAST_MESSAGE.get()
        assertEquals(er.eventStatus, TRIGGERED)
        assertEquals(er.payload, "P2")
        assertEquals(er.tenant, tenant)
        assertEquals(er.deliveryOption, PAYLOAD_ONLY)
        assertEquals(er.eventTime, eventTime)
    }

    private fun HttpClient.call(url: String, body: Any?, method: HttpMethod = Post): Pair<Int, String> {
        return runBlocking {
            client.call {
                url("$server$url")
                accept(Json)
                this.method = method
                body?.let { this.body = TextContent(it.json(), Json) }
            }.response.run { status.value to this.readText().apply { println("response: $this") } }
        }
    }
}

