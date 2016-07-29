package com.walmartlabs.components.scheduler.processors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.MessagePublisher;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.PublisherFactory;
import com.walmartlabs.components.scheduler.entities.Event;
import com.walmartlabs.components.scheduler.entities.EventResponse;
import com.walmartlabs.components.scheduler.entities.EventResponseMixin;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.SettableFuture.create;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.core.SpringContext.spring;
import static com.walmart.gmp.ingestion.platform.framework.utils.ConfigParser.parse;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.services.common.util.JsonUtil.convertToString;
import static com.walmartlabs.components.scheduler.entities.EventResponse.toResponse;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.now;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.MEDIA_TYPE_WILDCARD;

/**
 * Created by smalik3 on 6/21/16
 */
public class ProcessorRegistry implements EventProcessor<Event> {

    private static final Logger L = Logger.getLogger(ProcessorRegistry.class);
    private static final DevNullProcessor devNull = new DevNullProcessor();

    private final Map<String, ProcessorConfig> configs;

    public ProcessorRegistry(String configPath) throws Exception {
        this(configPath, false);
    }

    public ProcessorRegistry(String configPath, boolean eager) throws Exception {
        configs = parse(configPath, new TypeReference<List<ProcessorConfig>>() {
        }).stream().collect(Collectors.toMap(ProcessorConfig::getTenant, identity()));
        L.info("configs parsed: " + configs);
        if (eager) {
            L.info("creating the processors right away");
            configs.keySet().forEach(this::getOrCreate);
            L.info("all processors created");
        } else L.info("processors will be created when required");
    }

    private final Cache<String, EventProcessor<Event>> processorCache = CacheBuilder.newBuilder().build();

    private final TaskExecutor taskExecutor = new TaskExecutor(newHashSet(Exception.class));

    @Override
    public ListenableFuture<Event> process(Event event) {
        try {
            event.setProcessedAt(now(UTC));
            return catchingAsync(taskExecutor.async(() -> {
                        return getOrCreate(event.getTenant()).process(event);
                    }, "event-processor",
                    PROPS.getInteger("event.processor.max.retries", 3),
                    PROPS.getInteger("event.processor.initial.delay", 1),
                    PROPS.getInteger("event.processor.backoff.multiplier", 2),
                    SECONDS
            ), Exception.class, ex -> {
                L.error("error in event processor after multiple retries, this failure will be IGNORED and event is marked PROCESSED. event-id:" + event.id(), ex);
                return immediateFuture(event);
            });
        } catch (Exception e) {
            return immediateFailedFuture(getRootCause(e));
        }
    }

    private static final AsyncHttpClient ASYNC_HTTP_CLIENT = new AsyncHttpClient();

    public EventProcessor<Event> getOrCreate(String tenant) {
        try {
            final ProcessorConfig processorConfig = configs.get(tenant);
            if (processorConfig == null)
                return devNull;
            switch (processorConfig.getType()) {
                case KAFKA:
                    return processorCache.get(tenant, () -> {
                        final MessagePublisher<String, EventResponse, RecordMetadata> publisher =
                                new PublisherFactory(processorConfig.getProperties().get("topic").toString(),
                                        processorConfig.getProperties().get("configPath").toString(), true).create();
                        return e -> transform(publisher.publish(e.id().getEventId(), getEventResponse(e)),
                                new Function<RecordMetadata, Event>() {
                                    @Override
                                    public Event apply(RecordMetadata r) {
                                        if (L.isDebugEnabled())
                                            L.debug(format("event processed successfully, topic: %s, partition: %d, " +
                                                    "offset: %d, event: %s", r.topic(), r.partition(), r.offset(), e));
                                        return e;
                                    }
                                });
                    });
                case HTTP:
                    processorCache.get(tenant, () -> e -> {
                        final SettableFuture<Event> future = create();
                        try {
                            final AsyncHttpClient.BoundRequestBuilder builder = ASYNC_HTTP_CLIENT.
                                    preparePost(processorConfig.getProperties().get("url").toString())
                                    .setBody(convertToString(getEventResponse(e)));
                            @SuppressWarnings("unchecked")
                            final Map<String, String> headers = (Map<String, String>) processorConfig.getProperties().get("headers");
                            if (headers != null && !headers.isEmpty()) {
                                if (L.isDebugEnabled())
                                    L.debug("adding custom headers: " + headers);
                                headers.forEach(builder::setHeader);
                            }
                            builder.setHeader(ACCEPT, MEDIA_TYPE_WILDCARD).setHeader(CONTENT_TYPE, APPLICATION_JSON);
                            builder.execute(new AsyncCompletionHandler<Response>() {
                                @Override
                                public Response onCompleted(Response response) throws Exception {
                                    if (response.getStatusCode() == 200 || response.getStatusCode() == 204) {
                                        if (L.isDebugEnabled()) {
                                            L.debug(format("event processed successfully, response code: %d, response body: %s, event: %s",
                                                    response.getStatusCode(), response.getResponseBody(), e));
                                        }
                                        future.set(e);
                                    } else {
                                        future.setException(new RuntimeException(response.getResponseBody()));
                                    }
                                    return response;
                                }

                                @Override
                                public void onThrowable(Throwable t) {
                                    future.setException(getRootCause(t));
                                }
                            });
                            return future;
                        } catch (Exception ex) {
                            future.setException(getRootCause(ex));
                        }
                        return future;
                    });
                case CUSTOM_CLASS:
                    processorCache.get(tenant, () -> {
                        try {
                            @SuppressWarnings("unchecked")
                            final Class<EventProcessor<Event>> eventProcessorClass =
                                    (Class<EventProcessor<Event>>) Class.forName(processorConfig.getProperties().get("eventProcessorClass").toString());
                            return eventProcessorClass.newInstance();
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    });
                case CUSTOM_BEAN:
                    return processorCache.get(tenant, () -> {
                        @SuppressWarnings("unchecked")
                        final EventProcessor<Event> eventProcessor = spring().getBean(processorConfig.getProperties().get("eventProcessorBeanName").toString(), EventProcessor.class);
                        return eventProcessor;
                    });
                default:
                    throw new RuntimeException("no suitable processor found for type: " + processorConfig.getType() + ", and tenant: " + tenant);
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private EventResponse getEventResponse(Event e) {
        if (((EventResponseMixin) e).getEventResponse() != null)
            return ((EventResponseMixin) e).getEventResponse();
        else return toResponse(e);
    }

    public ProcessorConfig register(ProcessorConfig config) {
        checkArgument(config != null, "null processor config");
        checkArgument(config.getTenant() != null && config.getTenant().trim().length() > 0, "null or empty tenantId");
        checkArgument(config.getType() != null, "null processor type");
        checkArgument(config.getProperties() == null || config.getProperties().isEmpty(), "null or empty properties");
        L.info("registering new processor");
        final ProcessorConfig previous = configs.put(config.getTenant(), config);
        processorCache.invalidate(config.getTenant());
        return previous;
    }
}
