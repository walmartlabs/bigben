package com.walmartlabs.components.scheduler.core;

import org.apache.ignite.Ignite;
import org.apache.log4j.Logger;

import static org.apache.ignite.Ignition.start;

/**
 * Created by smalik3 on 3/23/16
 */
public class IgniteInstance {

    private static final Logger L = Logger.getLogger(IgniteInstance.class);

    private final Ignite ignite;

    public IgniteInstance(String template, String path) {
        L.info("starting the ignite instance: " + path);
        ignite = start(path);
        L.info("ignite instance started successfully!");
    }
}
