package com.walmartlabs.components.scheduler.processors;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Created by smalik3 on 3/8/16
 */
public interface EventProcessor<T> {

    ListenableFuture<T> process(T t);

}
