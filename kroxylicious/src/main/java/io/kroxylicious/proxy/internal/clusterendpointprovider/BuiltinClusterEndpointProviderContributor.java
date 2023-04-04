/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.clusterendpointprovider;

import io.kroxylicious.proxy.clusterendpointprovider.ClusterEndpointProviderContributor;
import io.kroxylicious.proxy.internal.clusterendpointprovider.StaticClusterEndpointProvider.StaticClusterEndpointProviderConfig;
import io.kroxylicious.proxy.service.BaseContributor;
import io.kroxylicious.proxy.service.ClusterEndpointProvider;

public class BuiltinClusterEndpointProviderContributor extends BaseContributor<ClusterEndpointProvider> implements ClusterEndpointProviderContributor {

    public static final BaseContributorBuilder<ClusterEndpointProvider> FILTERS = BaseContributor.<ClusterEndpointProvider> builder()
            .add("StaticCluster", StaticClusterEndpointProviderConfig.class, StaticClusterEndpointProvider::new);

    public BuiltinClusterEndpointProviderContributor() {
        super(FILTERS);
    }
}
