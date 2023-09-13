/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.ContributionManager;

import static io.kroxylicious.proxy.service.Context.wrap;

@SuppressWarnings("java:S6548")
public class ClusterNetworkAddressConfigProviderContributorManager {

    public static final ClusterNetworkAddressConfigProviderContributorManager INSTANCE = new ClusterNetworkAddressConfigProviderContributorManager();

    private ClusterNetworkAddressConfigProviderContributorManager() {
    }

    public static ClusterNetworkAddressConfigProviderContributorManager getInstance() {
        return INSTANCE;
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        try {
            return ContributionManager.INSTANCE.getDefinition(ClusterNetworkAddressConfigProviderContributor.class, shortName).configurationType();
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("No endpoint provider found for name '" + shortName + "'");
        }
    }

    public ClusterNetworkAddressConfigProvider getClusterEndpointConfigProvider(String shortName, BaseConfig baseConfig) {
        try {
            return ContributionManager.INSTANCE.getInstance(ClusterNetworkAddressConfigProviderContributor.class, shortName, wrap(baseConfig));
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("No endpoint provider found for name '" + shortName + "'");
        }
    }
}