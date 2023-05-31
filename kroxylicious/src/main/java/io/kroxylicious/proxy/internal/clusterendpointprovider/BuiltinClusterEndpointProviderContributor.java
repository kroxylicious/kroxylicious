/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.clusterendpointprovider;

import io.kroxylicious.proxy.clusterendpointprovider.ClusterEndpointProviderContributor;
import io.kroxylicious.proxy.internal.clusterendpointprovider.PortPerBrokerClusterEndpointConfigProvider.PortPerBrokerClusterEndpointProviderConfig;
import io.kroxylicious.proxy.internal.clusterendpointprovider.SniRoutingClusterEndpointConfigProvider.SniRoutingClusterEndpointProviderConfig;
import io.kroxylicious.proxy.service.BaseContributor;
import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;

public class BuiltinClusterEndpointProviderContributor extends BaseContributor<ClusterEndpointConfigProvider> implements ClusterEndpointProviderContributor {

    public static final BaseContributorBuilder<ClusterEndpointConfigProvider> FILTERS = BaseContributor.<ClusterEndpointConfigProvider> builder()
            .add("PortPerBroker", PortPerBrokerClusterEndpointProviderConfig.class, PortPerBrokerClusterEndpointConfigProvider::new)
            .add("SniRouting", SniRoutingClusterEndpointProviderConfig.class, SniRoutingClusterEndpointConfigProvider::new);

    public BuiltinClusterEndpointProviderContributor() {
        super(FILTERS);
    }
}
