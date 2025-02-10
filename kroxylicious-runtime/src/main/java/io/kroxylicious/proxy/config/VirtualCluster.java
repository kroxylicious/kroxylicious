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

import edu.umd.cs.findbugs.annotations.Nullable;

public record VirtualCluster(TargetCluster targetCluster,
                             @JsonProperty(required = true) ClusterNetworkAddressConfigProviderDefinition clusterNetworkAddressConfigProvider,
                             @JsonProperty() Optional<Tls> tls,
                             boolean logNetwork,
                             boolean logFrames,
                             @Nullable List<String> filters) {

}
