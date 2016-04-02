package com.walmartlabs.components.scheduler.core.hz;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.walmartlabs.components.scheduler.core.hz.HzEventReceiver.CountIncrementer;
import com.walmartlabs.components.scheduler.model.EventBucketStatusDO;

/**
 * Created by smalik3 on 4/1/16
 */
public class ObjectFactory implements DataSerializableFactory {

    public static final int SCHEDULER_FACTORY_ID = 1;

    public enum OBJECT_ID {
        EVENT_RECEIVER_ADD_EVENT,
        EVENT_BUCKET_DO
    }

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        switch (ObjectFactory.OBJECT_ID.values()[typeId]) {
            case EVENT_RECEIVER_ADD_EVENT:
                return new CountIncrementer();
            /*case EVENT_BUCKET_DO:
                return new EventBucketStatusDO();*/
            default:
                throw new IllegalArgumentException("unknown typeId: " + typeId);
        }
    }
}
