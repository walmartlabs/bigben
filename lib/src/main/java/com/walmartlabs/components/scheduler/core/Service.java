package com.walmartlabs.components.scheduler.core;

/**
 * Created by smalik3 on 4/1/16
 */
public interface Service {

    String name();

    void init();

    void execute();

    void destroy();

}
