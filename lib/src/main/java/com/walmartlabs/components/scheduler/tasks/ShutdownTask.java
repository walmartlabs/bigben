package com.walmartlabs.components.scheduler.tasks;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.walmartlabs.components.scheduler.entities.ObjectFactory;
import com.walmartlabs.components.scheduler.core.ScheduleScanner;

import java.io.IOException;
import java.util.concurrent.Callable;

import static com.walmart.gmp.ingestion.platform.framework.core.SpringContext.spring;
import static com.walmartlabs.components.scheduler.entities.ObjectFactory.SCHEDULER_FACTORY_ID;

/**
 * Created by smalik3 on 7/5/16
 */
public class ShutdownTask implements IdentifiedDataSerializable, Callable<Boolean> {

    @Override
    public int getFactoryId() {
        return SCHEDULER_FACTORY_ID;
    }

    @Override
    public int getId() {
        return ObjectFactory.OBJECT_ID.SHUTDOWN_TASK.ordinal();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }

    @Override
    public Boolean call() throws Exception {
        if (spring() != null) {
            spring().getBean(ScheduleScanner.class).shutdown();
            return true;
        } else {
            return false;
        }
    }
}
