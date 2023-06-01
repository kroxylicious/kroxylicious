/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Iterator;
import java.util.ServiceLoader;

import io.kroxylicious.proxy.clusterendpointprovider.ClusterEndpointProviderContributor;
import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;

public class ClusterEndpointConfigProviderContributorManager {

    private static final ClusterEndpointConfigProviderContributorManager INSTANCE = new ClusterEndpointConfigProviderContributorManager();

    private final ServiceLoader<ClusterEndpointProviderContributor> contributors;

    private ClusterEndpointConfigProviderContributorManager() {
        this.contributors = ServiceLoader.load(ClusterEndpointProviderContributor.class);
    }

    public static ClusterEndpointConfigProviderContributorManager getInstance() {
        return INSTANCE;
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        Iterator<ClusterEndpointProviderContributor> it = contributors.iterator();
        while (it.hasNext()) {
            ClusterEndpointProviderContributor contributor = it.next();
            Class<? extends BaseConfig> configType = contributor.getConfigType(shortName);
            if (configType != null) {
                return configType;
            }
        }

        throw new IllegalArgumentException("No endpoint provider found for name '" + shortName + "'");
    }

    public ClusterNetworkAddressConfigProvider getClusterEndpointConfigProvider(String shortName, BaseConfig baseConfig) {
        for (ClusterEndpointProviderContributor contributor : contributors) {
            var assigner = contributor.getInstance(shortName, baseConfig);
            if (assigner != null) {
                return assigner;
            }
        }

        throw new IllegalArgumentException("No endpoint provider found for name '" + shortName + "'");
    }
}
