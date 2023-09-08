/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.internal.ContributorManager;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;

public class ClusterNetworkAddressConfigProviderContributorManager
        extends ContributorManager<ClusterNetworkAddressConfigProvider, ClusterNetworkAddressConfigProviderContributor> {

    private static final ClusterNetworkAddressConfigProviderContributorManager INSTANCE = new ClusterNetworkAddressConfigProviderContributorManager();

    private ClusterNetworkAddressConfigProviderContributorManager() {
        super(ClusterNetworkAddressConfigProviderContributor.class, "endpoint provider");
    }

    public static ClusterNetworkAddressConfigProviderContributorManager getInstance() {
        return INSTANCE;
    }

}
