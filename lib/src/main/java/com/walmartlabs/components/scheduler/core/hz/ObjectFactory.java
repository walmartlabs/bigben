package com.walmartlabs.components.scheduler.core.hz;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.walmartlabs.components.scheduler.core.hz.HzEventReceiver.CACHED_PROCESSOR;

/**
 * Created by smalik3 on 4/1/16
 */
public class ObjectFactory implements DataSerializableFactory {

    public static final int SCHEDULER_FACTORY_ID = 1;

    public enum OBJECT_ID {
        EVENT_RECEIVER_ADD_EVENT,
        BULK_EVENT_TASK
    }

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        switch (ObjectFactory.OBJECT_ID.values()[typeId]) {
            case EVENT_RECEIVER_ADD_EVENT:
                return CACHED_PROCESSOR;
            case BULK_EVENT_TASK:
                return new BulkEventTask();
            /*case EVENT_BUCKET_DO:
                return new BucketDO();*/
            default:
                throw new IllegalArgumentException("unknown typeId: " + typeId);
        }
    }
}
