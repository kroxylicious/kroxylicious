/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.databind.util.StdConverter;

import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributorManager;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;

public class ClusterNetworkAddressConfigProviderConverter extends StdConverter<ClusterNetworkAddressConfigProviderDefinition, ClusterNetworkAddressConfigProvider> {

    @Override
    public ClusterNetworkAddressConfigProvider convert(ClusterNetworkAddressConfigProviderDefinition value) {
        return ClusterNetworkAddressConfigProviderContributorManager.getInstance()
                .getClusterEndpointConfigProvider(value.type(), value.config());

    }

}
