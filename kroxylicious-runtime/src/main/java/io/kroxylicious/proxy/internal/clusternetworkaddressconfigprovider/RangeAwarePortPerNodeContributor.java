/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.Context;

import edu.umd.cs.findbugs.annotations.NonNull;

public class RangeAwarePortPerNodeContributor implements
                                              ClusterNetworkAddressConfigProviderContributor<RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig> {

    @NonNull
    @Override
    public boolean requiresConfiguration() {
        return true;
    }

    @NonNull
    @Override
    public Class<? extends ClusterNetworkAddressConfigProvider> getServiceType() {
        return RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.class;
    }

    @NonNull
    @Override
    public Class<RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig> getConfigType() {
        return RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig.class;
    }

    @NonNull
    @Override
    public ClusterNetworkAddressConfigProvider createInstance(Context<RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig> context) {
        return new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(context.getConfig());
    }

}
