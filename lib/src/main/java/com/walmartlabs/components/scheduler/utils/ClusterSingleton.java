package com.walmartlabs.components.scheduler.utils;

import com.hazelcast.core.ILock;
import com.walmart.gmp.ingestion.platform.framework.core.Hz;
import com.walmart.gmp.ingestion.platform.framework.data.core.TaskExecutor;
import com.walmartlabs.components.scheduler.core.Service;
import org.apache.log4j.Logger;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTDOWN;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;
import static com.walmart.gmp.ingestion.platform.framework.core.Props.PROPS;
import static java.lang.Class.forName;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

/**
 * Created by smalik3 on 4/1/16
 */
public class ClusterSingleton {

    private static final Logger L = Logger.getLogger(ClusterSingleton.class);

    private final Service service;
    private final Hz hz;
    private final TaskExecutor taskExecutor;
    private final AtomicReference<String> listenerId = new AtomicReference<>();
    public static final Set<String> ACTIVE_SERVICES = newConcurrentHashSet();

    public ClusterSingleton(Service service, Hz hz) {
        this.service = service;
        this.hz = hz;
        final Set<Class<?>> exceptionClasses = asList(PROPS.getProperty(service.name() + ".retryable.exceptions",
                "java.lang.Exception").split(",")).stream().map(s -> {
            try {
                return forName(s.trim());
            } catch (ClassNotFoundException e) {
                throw new ExceptionInInitializerError(e);
            }
        }).collect(toSet());
        taskExecutor = new TaskExecutor(exceptionClasses);
        run();
    }

    private void run() {
        newSingleThreadExecutor().submit(() -> {
            try {
                currentThread().setName(service.name() + "_thread");
                final ILock clusterSingletonLock = hz.hz().getLock(service.name() + "_lock");
                clusterSingletonLock.lock();
                try {
                    L.info(format("cluster singleton ownership claimed, '%s[%s]' is the new owner: %s",
                            hz.hz().getCluster().getLocalMember(), currentThread().getName(), service.name()));
                    if (listenerId.get() != null) {
                        L.info("Adding the shutdown hook for cluster singleton: " + service.name());
                        listenerId.set(hz.hz().getLifecycleService().addLifecycleListener(event -> {
                            if (event.getState() == SHUTDOWN || event.getState() == SHUTTING_DOWN) {
                                L.info("node is shutting down, destroying the service: " + service.name());
                                try {
                                    service.destroy();
                                    ACTIVE_SERVICES.remove(service.name());
                                } catch (Exception e) {
                                    L.error("error in destroying the service: " + service.name(), e);
                                }
                            }
                        }));
                    }
                    L.info("starting the cluster singleton service: " + service.name());
                    service.init();
                    ACTIVE_SERVICES.add(service.name());
                    taskExecutor.async(() -> {
                                L.info("executing the cluster singleton service: " + service.name());
                                service.execute();
                                return immediateFuture(null);
                            }, service.name(),
                            PROPS.getInteger(service.name() + ".max.retries", 3),
                            PROPS.getInteger(service.name() + ".initial.delay", 0),
                            PROPS.getInteger(service.name() + ".backoff.multiplier", 1), SECONDS);
                } catch (Exception e) {
                    L.error("Error in executing cluster singleton service, the service is un-deployed, and will not be re-deployed any more: " + service.name(), e);
                    try {
                        service.destroy();
                    } catch (Exception e1) {
                        L.error("error in destroying the service: " + service.name(), e1);
                    }
                }
            } catch (Exception e) {
                run();
            }
        });
    }
}
