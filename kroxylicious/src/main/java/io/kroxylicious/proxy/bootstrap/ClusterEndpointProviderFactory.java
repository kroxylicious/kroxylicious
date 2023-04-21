/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import io.kroxylicious.proxy.config.ClusterEndpointConfigProviderDefinition;
import io.kroxylicious.proxy.internal.clusterendpointprovider.ClusterEndpointConfigProviderContributorManager;
import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;

public class ClusterEndpointProviderFactory {

    private ClusterEndpointConfigProviderDefinition clusterEndpointConfigProviderDefinition;

    public ClusterEndpointProviderFactory(ClusterEndpointConfigProviderDefinition clusterEndpointConfigProviderDefinition1) {
        this.clusterEndpointConfigProviderDefinition = clusterEndpointConfigProviderDefinition1;
    }

    public ClusterEndpointConfigProvider createClusterEndpointProvider() {
        return ClusterEndpointConfigProviderContributorManager.getInstance()
                .getClusterEndpointConfigProvider(clusterEndpointConfigProviderDefinition.type(), clusterEndpointConfigProviderDefinition.config());
    }
}
