/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.internal.clusterendpointprovider.ClusterEndpointProviderContributorManager;
import io.kroxylicious.proxy.service.ClusterEndpointProvider;

public class ClusterEndpointProviderFactory {

    private final Configuration config;
    private final VirtualCluster virtualCluster;

    public ClusterEndpointProviderFactory(Configuration config, VirtualCluster virtualCluster) {
        this.config = config;
        this.virtualCluster = virtualCluster;
    }

    public ClusterEndpointProvider createClusterEndpointProvider() {
        return ClusterEndpointProviderContributorManager.getInstance()
                .getClusterEndpointProvider(virtualCluster.clusterEndpointProvider().type(), virtualCluster.clusterEndpointProvider()
                        .config());
    }
}
