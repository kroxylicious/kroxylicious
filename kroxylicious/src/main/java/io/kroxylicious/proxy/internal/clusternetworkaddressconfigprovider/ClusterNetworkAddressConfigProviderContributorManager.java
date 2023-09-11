/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.Context;
import io.kroxylicious.proxy.service.ContributionManager;

import static io.kroxylicious.proxy.service.Context.wrap;

public class ClusterNetworkAddressConfigProviderContributorManager {

    private static final ClusterNetworkAddressConfigProviderContributorManager INSTANCE = new ClusterNetworkAddressConfigProviderContributorManager();
    private final ContributionManager<ClusterNetworkAddressConfigProvider, Context, ClusterNetworkAddressConfigProviderContributor> contributionManager;

    private ClusterNetworkAddressConfigProviderContributorManager() {
        contributionManager = new ContributionManager<>();
    }

    public static ClusterNetworkAddressConfigProviderContributorManager getInstance() {
        return INSTANCE;
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        return contributionManager.getDefinition(ClusterNetworkAddressConfigProviderContributor.class, shortName).configurationType();
    }

    public ClusterNetworkAddressConfigProvider getClusterEndpointConfigProvider(String shortName, BaseConfig baseConfig) {
        return contributionManager.getInstance(ClusterNetworkAddressConfigProviderContributor.class, shortName, wrap(baseConfig));
    }
}
