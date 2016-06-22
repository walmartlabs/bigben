package com.walmartlabs.components.scheduler.processors;

import java.util.Map;

/**
 * Created by smalik3 on 6/21/16
 */
public class ProcessorConfig {

    public enum Type {KAFKA, HTTP, CUSTOM_BEAN, CUSTOM_CLASS}

    private String tenant;
    private String config;
    private Type type;
    private Map<String, Object> properties;


    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "ProcessorConfig{" +
                "tenant='" + tenant + '\'' +
                ", config='" + config + '\'' +
                ", type=" + type +
                ", properties=" + properties +
                '}';
    }
}
