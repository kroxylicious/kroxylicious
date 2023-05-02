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

    private final AdminHttpConfiguration adminHttp;
    private final Map<String, VirtualCluster> virtualClusters;
    private final List<FilterDefinition> filters;
    private final List<MicrometerDefinition> micrometer;
    private final boolean useIoUring;

    public Configuration(AdminHttpConfiguration adminHttp,
                         Map<String, VirtualCluster> virtualClusters,
                         List<FilterDefinition> filters,
                         List<MicrometerDefinition> micrometer,
                         boolean useIoUring) {
        this.adminHttp = adminHttp;
        this.virtualClusters = virtualClusters;
        this.filters = filters;
        this.micrometer = micrometer;
        this.useIoUring = useIoUring;
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

    public boolean isUseIoUring() {
        return useIoUring;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Configuration [");
        sb.append("adminHttp=").append(adminHttp);
        sb.append(", virtualClusters=").append(virtualClusters);
        sb.append(", filters=").append(filters);
        sb.append(", micrometer=").append(micrometer);
        sb.append(", useIoUring=").append(useIoUring);
        sb.append(']');
        return sb.toString();
    }
}
