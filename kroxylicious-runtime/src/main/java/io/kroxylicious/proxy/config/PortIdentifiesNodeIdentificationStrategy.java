/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.NamedRangeSpec;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * PortIdentifiesNode.
 *
 * @param bootstrapAddress a {@link HostPort} defining the host and port of the bootstrap address. Required.
 * @param advertisedBrokerAddressPattern an address pattern used to form broker addresses.  It is addresses made from this pattern that are returned to the kafka
 *         client in the Metadata response so must be resolvable by the client.  One placeholder is supported: {@code $(nodeId)} which interpolates the node
 *         id into the address. If advertisedBrokerAddressPattern is omitted, it defaulted it based on the host name of {@code bootstrapAddress}. Optional.
 * @param nodeStartPort defines the starting range of port number that will be assigned to the brokers.  If omitted, it is defaulted to the port number
 *         of {@code bootstrapAddress + 1}. Optional.
 * @param nodeIdRanges defines the node id ranges present in the target cluster. If omitted, the system will behave as if a single range 0..2 is defined. Optional.
 */
public record PortIdentifiesNodeIdentificationStrategy(@JsonProperty(required = true) @NonNull HostPort bootstrapAddress,
                                                       @JsonProperty(required = false) @Nullable String advertisedBrokerAddressPattern,
                                                       @JsonProperty(required = false) @Nullable Integer nodeStartPort,
                                                       @JsonProperty(required = false) @Nullable List<NamedRange> nodeIdRanges)
        implements NodeIdentificationStrategy {

    static final NamedRange DEFAULT_RANGE = new NamedRange("default", 0, 3);

    public PortIdentifiesNodeIdentificationStrategy {
        Objects.requireNonNull(bootstrapAddress);
    }

    @Override
    public ClusterNetworkAddressConfigProviderDefinition get() {
        // This code is bridging to the old model
        var ranges = Optional.ofNullable(nodeIdRanges)
                .filter(Predicate.not(List::isEmpty))
                .orElse(List.of(DEFAULT_RANGE));

        var rangeSpecs = ranges.stream()
                .map(nir -> new NamedRangeSpec(nir.name(), new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.IntRangeSpec(nir.startInclusive(),
                        nir.endExclusive())))
                .toList();

        return new ClusterNetworkAddressConfigProviderDefinition(RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.class.getSimpleName(),
                new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(bootstrapAddress(),
                        advertisedBrokerAddressPattern(), nodeStartPort(), rangeSpecs));
    }

}
