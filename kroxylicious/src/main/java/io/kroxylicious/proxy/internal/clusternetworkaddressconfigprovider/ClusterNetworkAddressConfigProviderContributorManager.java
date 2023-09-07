/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Optional;
import java.util.ServiceLoader;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.InstanceFactory;

public class ClusterNetworkAddressConfigProviderContributorManager {

    private static final ClusterNetworkAddressConfigProviderContributorManager INSTANCE = new ClusterNetworkAddressConfigProviderContributorManager();

    private final ServiceLoader<ClusterNetworkAddressConfigProviderContributor> contributors;

    private ClusterNetworkAddressConfigProviderContributorManager() {
        this.contributors = ServiceLoader.load(ClusterNetworkAddressConfigProviderContributor.class);
    }

    public static ClusterNetworkAddressConfigProviderContributorManager getInstance() {
        return INSTANCE;
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        for (ClusterNetworkAddressConfigProviderContributor contributor : contributors) {
            Optional<InstanceFactory<ClusterNetworkAddressConfigProvider>> factory = contributor.getInstanceFactory(shortName);
            if (factory.isPresent()) {
                return factory.get().getConfigClass();
            }
        }

        throw new IllegalArgumentException("No endpoint provider found for name '" + shortName + "'");
    }

    public ClusterNetworkAddressConfigProvider getClusterEndpointConfigProvider(String shortName, BaseConfig baseConfig) {
        for (ClusterNetworkAddressConfigProviderContributor contributor : contributors) {
            Optional<InstanceFactory<ClusterNetworkAddressConfigProvider>> factory = contributor.getInstanceFactory(shortName);
            if (factory.isPresent()) {
                return factory.get().getInstance(baseConfig);
            }
        }

        throw new IllegalArgumentException("No endpoint provider found for name '" + shortName + "'");
    }
}
