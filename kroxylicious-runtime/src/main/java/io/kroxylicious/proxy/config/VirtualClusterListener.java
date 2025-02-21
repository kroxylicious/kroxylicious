/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A virtual cluster listener.
 *
 * @param name name of the listener
 * @param clusterNetworkAddressConfigProvider network config
 * @param tls tls settings
 */
public record VirtualClusterListener(@NonNull @JsonProperty(required = true) String name,
                                     @NonNull @JsonProperty(required = true) ClusterNetworkAddressConfigProviderDefinition clusterNetworkAddressConfigProvider,
                                     @NonNull Optional<Tls> tls) {

    public VirtualClusterListener {
        Objects.requireNonNull(name);
        Objects.requireNonNull(clusterNetworkAddressConfigProvider);
        Objects.requireNonNull(tls);
    }
}
