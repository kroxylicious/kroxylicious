/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.Context;

import edu.umd.cs.findbugs.annotations.NonNull;

public class SniRoutingContributor
                                   implements ClusterNetworkAddressConfigProviderContributor<SniRoutingClusterNetworkAddressConfigProviderConfig> {

    @NonNull
    @Override
    public boolean requiresConfiguration() {
        return true;
    }

    @NonNull
    @Override
    public Class<? extends ClusterNetworkAddressConfigProvider> getServiceType() {
        return SniRoutingClusterNetworkAddressConfigProvider.class;
    }

    @NonNull
    @Override
    public Class<SniRoutingClusterNetworkAddressConfigProviderConfig> getConfigType() {
        return SniRoutingClusterNetworkAddressConfigProviderConfig.class;
    }

    @NonNull
    @Override
    public ClusterNetworkAddressConfigProvider createInstance(Context<SniRoutingClusterNetworkAddressConfigProviderConfig> context) {
        return new SniRoutingClusterNetworkAddressConfigProvider(context.getConfig());
    }

}
