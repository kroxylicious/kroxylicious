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

    private final ProxyConfig proxy;

    private final AdminHttpConfiguration adminHttp;
    private final Map<String, Cluster> clusters;
    private final List<FilterDefinition> filters;

    public Configuration(ProxyConfig proxy, AdminHttpConfiguration adminHttp, Map<String, Cluster> clusters, List<FilterDefinition> filters) {
        this.proxy = proxy;
        this.adminHttp = adminHttp;
        this.clusters = clusters;
        this.filters = filters;
    }

    public ProxyConfig proxy() {
        return proxy;
    }

    public AdminHttpConfiguration adminHttpConfig() {
        return adminHttp;
    }

    public Map<String, Cluster> clusters() {
        return clusters;
    }

    public List<FilterDefinition> filters() {
        return filters;
    }
}
