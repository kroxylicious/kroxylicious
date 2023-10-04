/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Map;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.service.ContributionManager;

public class ClusterNetworkAddressConfigProviderDefinitionBuilder extends AbstractDefinitionBuilder<ClusterNetworkAddressConfigProviderDefinition> {
    public ClusterNetworkAddressConfigProviderDefinitionBuilder(String type) {
        super(type);
    }

    @Override
    protected ClusterNetworkAddressConfigProviderDefinition buildInternal(String type, Map<String, Object> config) {
        Class<?> result = ContributionManager.INSTANCE.getDefinition(ClusterNetworkAddressConfigProviderContributor.class, type).configurationType();

        return new ClusterNetworkAddressConfigProviderDefinition(type, mapper.convertValue(config, result));
    }
}
