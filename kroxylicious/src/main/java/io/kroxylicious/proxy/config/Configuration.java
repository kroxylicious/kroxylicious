/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Map;

import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;

public class Configuration {

    private final DefaultProxyConfig proxy;

    private final AdminHttpConfiguration adminHttp;
    private final Map<String, VirtualCluster> virtualClusters;
    private final List<FilterDefinition> filters;
    private final List<MicrometerDefinition> micrometer;

    public Configuration(DefaultProxyConfig proxy,
                         AdminHttpConfiguration adminHttp,
                         Map<String, VirtualCluster> virtualClusters,
                         List<FilterDefinition> filters,
                         List<MicrometerDefinition> micrometer) {
        this.proxy = proxy;
        this.adminHttp = adminHttp;
        this.virtualClusters = virtualClusters;
        this.filters = filters;
        this.micrometer = micrometer;
    }

    public DefaultProxyConfig proxy() {
        return proxy;
    }

    public AdminHttpConfiguration adminHttpConfig() {
        return adminHttp;
    }

    public Map<String, VirtualCluster> virtualClusters() {
        return virtualClusters;
    }

    public List<FilterDefinition> filters() {
        return filters;
    }

    public List<MicrometerDefinition> getMicrometer() {
        return micrometer == null ? List.of() : micrometer;
    }
}
