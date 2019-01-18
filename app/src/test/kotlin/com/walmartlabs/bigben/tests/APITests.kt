package com.walmartlabs.bigben.tests

import com.walmartlabs.bigben.BigBen.module
import com.walmartlabs.bigben.api.EventService
import com.walmartlabs.bigben.app.AppRun
import com.walmartlabs.bigben.app.EventGenerator
import com.walmartlabs.bigben.entities.EventLoader
import com.walmartlabs.bigben.entities.EventStatus.PROCESSED
import com.walmartlabs.bigben.processors.ProcessorConfig
import com.walmartlabs.bigben.processors.ProcessorConfig.Type.CUSTOM_CLASS
import com.walmartlabs.bigben.utils.json
import com.walmartlabs.bigben.utils.typeRefJson
import io.ktor.client.HttpClient
import io.ktor.client.call.call
import io.ktor.client.engine.apache.Apache
import io.ktor.client.request.accept
import io.ktor.client.request.post
import io.ktor.client.request.url
import io.ktor.content.TextContent
import io.ktor.http.ContentType.Application.Json
import io.ktor.http.HttpMethod.Companion.Post
import kotlinx.coroutines.runBlocking
import org.testng.annotations.AfterClass
import org.testng.annotations.Test
import java.time.ZonedDateTime
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class APITests {

    companion object {
        init {
            System.setProperty("bigben.props", "file://bigben-api-test.yaml")
            EventService.DEBUG_FLAG.set(false)
            thread { AppRun.main(emptyArray()) }
            AppRun.latch.await()
        }
    }

    val client = HttpClient(Apache)

    @AfterClass
    fun teardown() {
        client.close()
    }

    @Test
    fun `test events at the same time`() {
        val server = "http://localhost:8080"

        assertEquals(runBlocking {
            client.call {
                url("$server/events/tenant/register")
                accept(Json)
                method = Post
                body = TextContent(
                    ProcessorConfig(
                        "test", CUSTOM_CLASS,
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
                body = TextContent(EventGenerator.EventGeneration("PT1M", "PT0S", 1000, "java").json(), Json)
            }
        }

        // schedule 1000 events at exactly same time at the start of the minute + 30 seconds:
        val bucket = runBlocking {
            client.post<String> {
                url("$server/generation/random")
                accept(Json)
                body = TextContent(EventGenerator.EventGeneration("PT1M30S", "PT0S", 1000, "java").json(), Json)
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
                l =
                        module<EventLoader>().load(bucket, 0, 400, l.second.last().eventTime!!, l.second.last().id!!, l.first)
                            .get()
            }
        }
    }

}

