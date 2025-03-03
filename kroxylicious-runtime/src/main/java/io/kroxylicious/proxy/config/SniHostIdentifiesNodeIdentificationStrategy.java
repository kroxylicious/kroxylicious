/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * SniHostIdentifiesNode.
 *
 * @param bootstrapAddress a {@link HostPort} defining the host and port of the bootstrap address (required).
 * @param advertisedBrokerAddressPattern a pattern used to derive broker addresses. It is addresses derived from this pattern that are sent to the Kafka client (so they
 *        must be resolvable and routable from the client's network).  A port number can be included.  If the port number is not included, the port number assigned
 *        to the bootstrapAddress is used.  One pattern is supported: {@code $(nodeId)} which interpolates the node id into the address (required).
 */
public record SniHostIdentifiesNodeIdentificationStrategy(@NonNull @JsonProperty(required = true) HostPort bootstrapAddress,
                                                          @NonNull @JsonProperty(required = true) String advertisedBrokerAddressPattern)
        implements NodeIdentificationStrategy {
    public SniHostIdentifiesNodeIdentificationStrategy {
        Objects.requireNonNull(bootstrapAddress);
        Objects.requireNonNull(advertisedBrokerAddressPattern);
    }

    @SuppressWarnings("removal")
    public ClusterNetworkAddressConfigProviderDefinition get() {
        // This code is bridging to the old model
        return new ClusterNetworkAddressConfigProviderDefinition(SniRoutingClusterNetworkAddressConfigProvider.class.getSimpleName(),
                new SniRoutingClusterNetworkAddressConfigProviderConfig(bootstrapAddress(),
                        null, advertisedBrokerAddressPattern()));
    }
}
