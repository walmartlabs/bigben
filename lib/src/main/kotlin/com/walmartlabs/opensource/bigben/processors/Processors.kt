package com.walmartlabs.opensource.bigben.processors

import com.google.common.base.Throwables.getStackTraceAsString
import com.google.common.cache.CacheBuilder
import com.google.common.net.HttpHeaders.ACCEPT
import com.google.common.net.HttpHeaders.CONTENT_TYPE
import com.google.common.net.MediaType.ANY_TYPE
import com.google.common.net.MediaType.JSON_UTF_8
import com.google.common.util.concurrent.Futures.immediateFailedFuture
import com.google.common.util.concurrent.Futures.immediateFuture
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.SettableFuture
import com.ning.http.client.AsyncCompletionHandler
import com.ning.http.client.AsyncHttpClient
import com.ning.http.client.Response
import com.walmartlabs.opensource.bigben.entities.Event
import com.walmartlabs.opensource.bigben.entities.EventStatus.*
import com.walmartlabs.opensource.bigben.extns.*
import com.walmartlabs.opensource.bigben.processors.ProcessorConfig.Type.*
import com.walmartlabs.opensource.bigben.utils.Props
import toResponse
import java.io.Serializable
import java.lang.String.format
import java.util.concurrent.ExecutionException


/**
 * Created by smalik3 on 2/24/18
 */
typealias EventProcessor<T> = (t: T) -> ListenableFuture<T>

data class ProcessorConfig(var tenant: String? = null, var type: Type? = null, var props: Map<String, Any>? = null) : Serializable {
    enum class Type {
        KAFKA, HTTP, CUSTOM_CLASS
    }
}

object ProcessorRegistry : EventProcessor<Event> {

    val instance = this

    private val l = logger<ProcessorRegistry>()
    private val devNull = DevNull()
    private val ASYNC_HTTP_CLIENT = AsyncHttpClient()

    private val configs = typeRefJson<List<ProcessorConfig>>(Props.string("processor.configs", "[]")).associate { it.tenant!! to it }.toMutableMap()
    private val processorCache = CacheBuilder.newBuilder().build<String, EventProcessor<Event>>()

    init {
        if (l.isInfoEnabled) l.info("configs parsed: {}", configs)
        if (Props.boolean("processor.eager.loading", false)) {
            if (l.isInfoEnabled) l.info("creating the processors right away")
            configs.forEach { getOrCreate(it.value) }
            if (l.isInfoEnabled) l.info("all processors created")
        } else
            if (l.isInfoEnabled) l.info("processors will be created when required")
    }

    override fun invoke(e: Event): ListenableFuture<Event> {
        try {
            e.status = TRIGGERED
            e.error = null
            e.processedAt = nowUTC()

            return { getOrCreate(configs[e.tenant]).invoke(e) }.retriable("processor-e-id: ${e.id}",
                    Props.int("event.processor.max.retries", 3),
                    Props.int("event.processor.initial.delay", 1),
                    Props.int("event.processor.backoff.multiplier", 2)).apply {
                transform {
                    if (TRIGGERED == e.status) {
                        e.status = e.error?.let { PROCESSED } ?: ERROR
                    }
                }.catching {
                    l.error("error in processing event by processor after multiple retries, will be retried later if within " +
                            "'events.backlog.check.limit', e-id: ${e.id}", it.rootCause())
                    e.status = ERROR
                    e.error = it?.let { getStackTraceAsString(it) } ?: "null error"
                }
            }
        } catch (ex: Exception) {
            e.status = ERROR
            e.error = getStackTraceAsString(ex.rootCause()!!)
            return immediateFailedFuture<Event>(ex.rootCause()!!)
        }
    }

    private fun getOrCreate(processorConfig: ProcessorConfig?): EventProcessor<Event> {
        return try {
            when (processorConfig?.type) {
                KAFKA -> processorCache.get(processorConfig.tenant!!) {
                    TODO("kafka")
                }
                HTTP -> {
                    processorCache.get(processorConfig.tenant!!) {
                        {
                            SettableFuture.create<Event>().apply {
                                try {
                                    val builder = ASYNC_HTTP_CLIENT.preparePost(processorConfig.props!!["url"].toString()).setBody(it.toResponse().json())
                                    @Suppress("UNCHECKED_CAST")
                                    (processorConfig.props!!["headers"] as? Map<String, String>)?.let {
                                        if (l.isDebugEnabled) l.debug("adding custom headers: {}", it)
                                        it.forEach { builder.setHeader(it.key, it.value) }
                                    }
                                    builder.setHeader(ACCEPT, ANY_TYPE.toString()).setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                                    builder.execute(object : AsyncCompletionHandler<Response>() {
                                        override fun onCompleted(response: Response): Response {
                                            val code = response.statusCode
                                            if (code in 200..299 || code in 400..499) {
                                                if (code < 400) {
                                                    if (l.isDebugEnabled)
                                                        l.debug(format("event processed successfully, response code: {}, response body: {}, event: {}", code, response.responseBody, it))
                                                } else {
                                                    l.warn(format("got a 'bad request' response with status code: {}, event will not be retried anymore", code))
                                                    it.error = response.responseBody
                                                }
                                                set(it)
                                            } else {
                                                setException(RuntimeException(response.responseBody))
                                            }
                                            return response
                                        }

                                        override fun onThrowable(t: Throwable) {
                                            setException(t.rootCause()!!)
                                        }
                                    })
                                } catch (ex: Exception) {
                                    setException(ex.rootCause()!!)
                                }
                            }
                        }
                    }
                }
                CUSTOM_CLASS -> processorCache.get(processorConfig.tenant!!) {
                    try {
                        @Suppress("UNCHECKED_CAST")
                        (Class.forName(processorConfig.props!!["eventProcessorClass"].toString()) as Class<EventProcessor<Event>>).
                                getConstructor(Map::class.java).newInstance(processorConfig.props)
                    } catch (ex: Exception) {
                        throw RuntimeException(ex.rootCause())
                    }
                }
                null -> devNull
            }
        } catch (e: ExecutionException) {
            throw RuntimeException(e)
        }
    }

    fun register(config: ProcessorConfig?): ProcessorConfig? {
        require(config != null) { "null processor config" }
        require(config!!.tenant != null && config.tenant!!.trim().isNotEmpty()) { "null or empty tenantId" }
        require(config.type != null) { "null processor type" }
        require(config.props != null && !config.props!!.isEmpty()) { "null or empty properties" }
        if (l.isInfoEnabled) l.info("registering new processor")
        val previous = configs.put(config.tenant!!, config)
        processorCache.invalidate(config.tenant!!)
        return previous
    }

    fun registeredTenants(): Set<String> {
        return configs.keys.toSet()
    }

    fun registeredConfigs(): Map<String, ProcessorConfig> {
        return configs
    }

    class DevNull : EventProcessor<Event> {
        override fun invoke(t: Event): ListenableFuture<Event> {
            l.warn("routing event response with no tenant to /dev/null: {}", t.eventResponse)
            t.error = "/dev/null"
            t.status = PROCESSED
            return immediateFuture(t)
        }
    }
}
