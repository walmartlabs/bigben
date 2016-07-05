package com.walmartlabs.components.scheduler.tasks;

import com.walmart.gmp.ingestion.platform.framework.jobs.SerializableCallable;

import static com.walmartlabs.components.scheduler.utils.ClusterSingleton.ACTIVE_SERVICES;

/**
 * Created by smalik3 on 7/5/16
 */
public class StatusTask implements SerializableCallable<String> {

    private final String serviceName;

    public StatusTask(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public String call() throws Exception {
        return ACTIVE_SERVICES.contains(serviceName) ? "Master" : "Slave";
    }
}
