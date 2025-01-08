/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProviderService;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public record VirtualCluster(TargetCluster targetCluster,
                             @JsonProperty(required = true) ClusterNetworkAddressConfigProviderDefinition clusterNetworkAddressConfigProvider,
                             @JsonProperty() Optional<Tls> tls,
                             boolean logNetwork,
                             boolean logFrames,
                             @Nullable List<String> filterRefs) {

    public io.kroxylicious.proxy.model.VirtualCluster toVirtualClusterModel(@NonNull PluginFactoryRegistry pfr,
                                                                            @NonNull List<NamedFilterDefinition> filterDefinitions,
                                                                            @NonNull String virtualClusterNodeName) {

        return new io.kroxylicious.proxy.model.VirtualCluster(virtualClusterNodeName,
                targetCluster(),
                toClusterNetworkAddressConfigProviderModel(pfr),
                tls(),
                logNetwork(),
                logFrames(),
                filterDefinitions);
    }

    private ClusterNetworkAddressConfigProvider toClusterNetworkAddressConfigProviderModel(@NonNull PluginFactoryRegistry registry) {
        ClusterNetworkAddressConfigProviderService provider = registry.pluginFactory(ClusterNetworkAddressConfigProviderService.class)
                .pluginInstance(clusterNetworkAddressConfigProvider.type());
        return provider.build(clusterNetworkAddressConfigProvider.config());
    }
}
