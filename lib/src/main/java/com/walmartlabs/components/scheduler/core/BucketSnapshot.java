package com.walmartlabs.components.scheduler.core;

import com.walmartlabs.components.scheduler.entities.Status;

import java.time.ZonedDateTime;
import java.util.BitSet;

import static com.walmartlabs.components.scheduler.core.BucketManager.L;
import static com.walmartlabs.components.scheduler.entities.Status.PROCESSED;
import static java.lang.String.format;

/**
 * Created by smalik3 on 8/6/16
 */
class BucketSnapshot {

    private static final BitSet EMPTY = new BitSet();

    private final ZonedDateTime id;
    private final long count;
    private final BitSet awaiting;
    private final BitSet processing;

    BucketSnapshot(ZonedDateTime id, long count, BitSet awaiting, BitSet processing) {
        this.id = id;
        this.count = count;
        this.awaiting = awaiting;
        this.processing = processing;
    }

    BucketSnapshot(ZonedDateTime id, long count, int shardSize, String status) {
        this.id = id;
        this.count = count;
        this.processing = new BitSet();
        final int shards = shards(shardSize);
        awaiting = count == 0 || PROCESSED.name().equals(status) ? EMPTY : new BitSet(shards);
        if (awaiting != EMPTY) {
            awaiting.set(0, shards);
        }
        if (count == 0)
            L.info(format("bucket: %s => empty, no events", id));
        else if (awaiting == EMPTY)
            L.info(format("bucket: %s => already done", id));
        else
            L.info(format("bucket: %s => has %d events, resulting in %d shards", id, count, shards));
    }

    BitSet awaiting() {
        return awaiting;
    }

    BitSet processing() {
        return processing;
    }

    long count() {
        return count;
    }

    ZonedDateTime id() {
        return id;
    }

    void done(int shard, Status status) {
        processing.clear(shard);
        switch (status) {
            case PROCESSED:
                L.info(format("shard: %s[%s] finished successfully", id, shard));
                awaiting.clear(shard);
                break;
            case ERROR:
                L.info(format("shard: %s[%s] finished with error", id, shard));
                awaiting.set(shard);
                break;
            default:
                throw new IllegalArgumentException("invalid status value: " + status);
        }
    }

    void processing(int shard) {
        awaiting.clear(shard);
        processing.set(shard);
    }

    private int shards(int shardSize) {
        return (int) (count % shardSize == 0 ? count / shardSize : count / shardSize + 1);
    }

    @Override
    public String toString() {
        return "BucketSnapshot{" +
                "bucket=" + id +
                ", count=" + count +
                ", awaiting=" + awaiting +
                ", processing=" + processing +
                '}';
    }
}
