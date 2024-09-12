/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.Context;

import edu.umd.cs.findbugs.annotations.NonNull;

public class PortPerBrokerContributor implements
                                      ClusterNetworkAddressConfigProviderContributor<PortPerBrokerClusterNetworkAddressConfigProviderConfig> {

    @NonNull
    @Override
    public boolean requiresConfiguration() {
        return true;
    }

    @NonNull
    @Override
    public Class<? extends ClusterNetworkAddressConfigProvider> getServiceType() {
        return PortPerBrokerClusterNetworkAddressConfigProvider.class;
    }

    @NonNull
    @Override
    public Class<PortPerBrokerClusterNetworkAddressConfigProviderConfig> getConfigType() {
        return PortPerBrokerClusterNetworkAddressConfigProviderConfig.class;
    }

    @NonNull
    @Override
    public ClusterNetworkAddressConfigProvider createInstance(
            Context<PortPerBrokerClusterNetworkAddressConfigProviderConfig> context
    ) {
        return new PortPerBrokerClusterNetworkAddressConfigProvider(context.getConfig());
    }

}
