/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.ContributionManager;

import static io.kroxylicious.proxy.service.Context.wrap;

public record VirtualCluster(
        TargetCluster targetCluster,
        @JsonProperty(required = true)
        ClusterNetworkAddressConfigProviderDefinition clusterNetworkAddressConfigProvider,

        @JsonProperty()
        Optional<Tls> tls,
        boolean logNetwork,
        boolean logFrames
) {
    public io.kroxylicious.proxy.model.VirtualCluster toVirtualClusterModel(String virtualClusterNodeName) {
        return new io.kroxylicious.proxy.model.VirtualCluster(
                virtualClusterNodeName,
                targetCluster(),
                toClusterNetworkAddressConfigProviderModel(),
                tls(),
                logNetwork(),
                logFrames()
        );
    }

    private ClusterNetworkAddressConfigProvider toClusterNetworkAddressConfigProviderModel() {
        String shortName = clusterNetworkAddressConfigProvider().type();
        return ContributionManager.INSTANCE.createInstance(
                ClusterNetworkAddressConfigProviderContributor.class,
                shortName,
                wrap(this.clusterNetworkAddressConfigProvider().config())
        );
    }
}
