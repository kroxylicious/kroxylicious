/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterEndpointProvider;

public class StaticClusterEndpointProvider implements ClusterEndpointProvider {

    private final StaticClusterEndpointProviderConfig config;

    public StaticClusterEndpointProvider(StaticClusterEndpointProviderConfig config) {
        this.config = config;
    }

    @Override
    public String getClusterBootstrapAddress() {
        return config.bootstrapAddress;
    }

    public static class StaticClusterEndpointProviderConfig extends BaseConfig {
        private final String bootstrapAddress;

        public StaticClusterEndpointProviderConfig(String bootstrapAddress) {
            this.bootstrapAddress = bootstrapAddress;
        }
    }

}
