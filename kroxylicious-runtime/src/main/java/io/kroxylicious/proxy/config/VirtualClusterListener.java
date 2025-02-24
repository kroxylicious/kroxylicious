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
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A virtual cluster listener.
 *
 * @param name name of the listener
 * @param portIdentifiesNode network config
 * @param sniHostIdentifiesNode network config
 * @param tls tls settings
 */
public record VirtualClusterListener(@NonNull @JsonProperty(required = true) String name,
                                     @Nullable @JsonProperty(required = false) PortIdentifiesNodeIdentificationStrategy portIdentifiesNode,
                                     @Nullable @JsonProperty(required = false) SniHostIdentifiesNodeIdentificationStrategy sniHostIdentifiesNode,
                                     @NonNull Optional<Tls> tls) {

    public VirtualClusterListener {
        Objects.requireNonNull(name);
        Objects.requireNonNull(tls);

        if (!(portIdentifiesNode == null) ^ (sniHostIdentifiesNode == null)) {
            throw new IllegalConfigurationException("Must specify either portIdentifiesNode or sniHostIdentifiesNode (virtual cluster listener %s)".formatted(name));
        }

        if (sniHostIdentifiesNode != null && tls.isEmpty()) {
            throw new IllegalConfigurationException("When using 'sniHostIdentifiesNode', 'tls' must be provided (virtual cluster listener %s)".formatted(name));
        }
    }

    public ClusterNetworkAddressConfigProviderDefinition clusterNetworkAddressConfigProvider() {
        if (portIdentifiesNode() != null) {
            return portIdentifiesNode().get();
        }
        else if (sniHostIdentifiesNode() != null) {
            return sniHostIdentifiesNode().get();
        }
        else {
            throw new IllegalStateException("Failed to create provider for " + this);
        }
    }

    @Override
    public String toString() {
        return "VirtualClusterListener{" +
                "name='" + name + '\'' +
                ", portIdentifiesNode=" + portIdentifiesNode +
                ", sniHostIdentifiesNode=" + sniHostIdentifiesNode +
                ", tls=" + tls +
                '}';
    }
}
