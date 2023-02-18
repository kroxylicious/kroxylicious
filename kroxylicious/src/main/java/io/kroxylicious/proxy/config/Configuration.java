/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Map;

import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;
import io.kroxylicious.proxy.config.micrometer.MicrometerConfiguration;

public class Configuration {

    private final ProxyConfig proxy;

    private final AdminHttpConfiguration adminHttp;
    private final MicrometerConfiguration micrometer;
    private final Map<String, Cluster> clusters;
    private final AddressManagerDefinition addressManager;
    private final List<FilterDefinition> filters;

    public Configuration(ProxyConfig proxy,
                         AdminHttpConfiguration adminHttp,
                         MicrometerConfiguration micrometer,
                         Map<String, Cluster> clusters,
                         AddressManagerDefinition addressManager, List<FilterDefinition> filters) {
        this.proxy = proxy;
        this.adminHttp = adminHttp;
        this.micrometer = micrometer;
        this.clusters = clusters;
        this.addressManager = addressManager;
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

    public MicrometerConfiguration micrometerConfig() {
        return micrometer == null ? MicrometerConfiguration.empty() : micrometer;
    }

    public AddressManagerDefinition getAddressManager() {
        return addressManager;
    }
}
