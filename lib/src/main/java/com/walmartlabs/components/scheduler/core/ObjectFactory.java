package com.walmartlabs.components.scheduler.core;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.walmartlabs.components.scheduler.core.EventReceiver.CACHED_PROCESSOR;
import static com.walmartlabs.components.scheduler.services.EventService.SHUTDOWN_TASK;

/**
 * Created by smalik3 on 4/1/16
 */
public class ObjectFactory implements DataSerializableFactory {

    public static final int SCHEDULER_FACTORY_ID = 1;

    public enum OBJECT_ID {
        EVENT_RECEIVER_ADD_EVENT,
        BULK_EVENT_TASK,
        SHUTDOWN_TASK,
        SHARD_STATUS,
        SHARD_STATUS_LIST;
    }

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        switch (ObjectFactory.OBJECT_ID.values()[typeId]) {
            case EVENT_RECEIVER_ADD_EVENT:
                return CACHED_PROCESSOR;
            case BULK_EVENT_TASK:
                return new BulkShardTask();
            case SHUTDOWN_TASK:
                return SHUTDOWN_TASK;
            case SHARD_STATUS:
                return new ShardStatus();
            case SHARD_STATUS_LIST:
                return new ShardStatusList();
            /*case EVENT_BUCKET_DO:
                return new BucketDO();*/
            default:
                throw new IllegalArgumentException("unknown typeId: " + typeId);
        }
    }
}
