/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Iterator;
import java.util.ServiceLoader;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.ContributorContext;

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
        Iterator<ClusterNetworkAddressConfigProviderContributor> it = contributors.iterator();
        while (it.hasNext()) {
            ClusterNetworkAddressConfigProviderContributor contributor = it.next();
            Class<? extends BaseConfig> configType = contributor.getConfigType(shortName);
            if (configType != null) {
                return configType;
            }
        }

        throw new IllegalArgumentException("No endpoint provider found for name '" + shortName + "'");
    }

    public ClusterNetworkAddressConfigProvider getClusterEndpointConfigProvider(String shortName, BaseConfig baseConfig) {
        for (ClusterNetworkAddressConfigProviderContributor contributor : contributors) {
            var assigner = contributor.getInstance(shortName, baseConfig, ContributorContext.instance());
            if (assigner != null) {
                return assigner;
            }
        }

        throw new IllegalArgumentException("No endpoint provider found for name '" + shortName + "'");
    }
}
