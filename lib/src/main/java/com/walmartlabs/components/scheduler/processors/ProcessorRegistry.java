package com.walmartlabs.components.scheduler.processors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.MessagePublisher;
import com.walmart.gmp.ingestion.platform.framework.messaging.kafka.PublisherFactory;
import com.walmartlabs.components.scheduler.core.EventProcessor;
import com.walmartlabs.components.scheduler.model.Event;
import com.walmartlabs.components.scheduler.model.EventResponse;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.SettableFuture.create;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static com.walmart.gmp.ingestion.platform.framework.core.SpringContext.spring;
import static com.walmart.gmp.ingestion.platform.framework.utils.ConfigParser.parse;
import static com.walmart.platform.soa.common.exception.util.ExceptionUtil.getRootCause;
import static com.walmart.services.common.util.JsonUtil.convertToString;
import static com.walmartlabs.components.scheduler.model.EventResponse.toResponse;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.now;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;

/**
 * Created by smalik3 on 6/21/16
 */
public class ProcessorRegistry implements EventProcessor<Event> {

    private static final Logger L = Logger.getLogger(ProcessorRegistry.class);

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

    private final Cache<String, EventProcessor<Event>> producerCache = CacheBuilder.newBuilder().build();

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
                L.error("error in event processor, this error will be IGNORED. event-id:" + event.id(), ex);
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
            checkArgument(processorConfig != null, "no config for tenant: " + tenant);
            switch (processorConfig.getType()) {
                case KAFKA:
                    return producerCache.get(tenant, () -> {
                        final MessagePublisher<String, EventResponse, EventResponse> publisher =
                                new PublisherFactory(processorConfig.getProperties().get("topic").toString(), processorConfig.getConfig()).create();
                        return e -> transform(publisher.publish(e.id().getEventId(), toResponse(e)), (Function<EventResponse, Event>) $ -> e);
                    });
                case HTTP:
                    producerCache.get(tenant, () -> e -> {
                        try {
                            final com.ning.http.client.ListenableFuture<Response> f = ASYNC_HTTP_CLIENT.
                                    preparePost(processorConfig.getProperties().get("url").toString()).setBody(convertToString(toResponse(e))).execute();
                            final SettableFuture<Event> future = create();
                            f.addListener(() -> {
                                try {
                                    final Response response = f.get();
                                    if (response.getStatusCode() == 200) {
                                        future.set(e);
                                    } else {
                                        future.setException(new RuntimeException(response.getResponseBody()));
                                    }
                                } catch (Exception e1) {
                                    future.setException(getRootCause(e1));
                                }
                            }, directExecutor());
                            return future;
                        } catch (IOException ioe) {
                            return immediateFailedFuture(getRootCause(ioe));
                        }
                    });
                case CUSTOM_CLASS:
                    producerCache.get(tenant, () -> {
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
                    return producerCache.get(tenant, () -> {
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

    public void register(ProcessorConfig config) {

    }
}
