package com.walmartlabs.components.scheduler.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.walmart.gmp.ingestion.platform.framework.utils.ConfigParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by smalik3 on 3/30/16
 */
public class ConfiguredIPList extends ArrayList<String> {
    public ConfiguredIPList(String configPath) throws Exception {
        final List<String> ipAddresses = ConfigParser.parse(configPath, new TypeReference<List<String>>() {
        });
        ipAddresses.forEach(this::add);
    }
}
