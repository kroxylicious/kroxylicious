/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.EXPECTED_TOKEN_SET;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validatePortSpecifier;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validateStringContainsOnlyExpectedTokens;

public class RangeAwarePortPerNodeClusterNetworkAddressConfigProvider implements ClusterNetworkAddressConfigProvider {

    private final HostPort bootstrapAddress;
    private final String nodeAddressPattern;
    private final int nodeStartPort;
    private final Set<Integer> exclusivePorts;
    private final IndexedIntSet nodeIdIndex;

    /**
     * Creates the provider.
     *
     * @param config configuration
     */
    public RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig config) {
        this.bootstrapAddress = config.bootstrapAddress;
        this.nodeAddressPattern = config.nodeAddressPattern;
        this.nodeStartPort = config.nodeStartPort;
        this.nodeIdIndex = config.nodeIdIndex;

        int nodeEndPortExclusive = nodeStartPort + config.nodeIdIndex.size();
        var exclusivePorts = IntStream.range(nodeStartPort, nodeEndPortExclusive).boxed().collect(Collectors.toCollection(HashSet::new));
        exclusivePorts.add(bootstrapAddress.port());
        this.exclusivePorts = Collections.unmodifiableSet(exclusivePorts);
    }

    @Override
    public HostPort getClusterBootstrapAddress() {
        return this.bootstrapAddress;
    }

    @Override
    public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
        if (!nodeIdIndex.contains(nodeId)) {
            throw new IllegalArgumentException(
                    "Cannot generate node address for node id %d as it is not contained in the ranges defined for provider with downstream bootstrap %s"
                            .formatted(
                                    nodeId,
                                    bootstrapAddress));
        }
        int port = nodeStartPort + nodeIdIndex.indexOf(nodeId);
        return new HostPort(BrokerAddressPatternUtils.replaceLiteralNodeId(nodeAddressPattern, nodeId), port);
    }

    @Override
    public Set<Integer> getExclusivePorts() {
        return this.exclusivePorts;
    }

    @Override
    public Map<Integer, HostPort> discoveryAddressMap() {
        return nodeIdIndex.values().stream()
                .collect(Collectors.toMap(Function.identity(), this::getBrokerAddress));
    }

    /**
     * @param name arbitrary name for this range, no functional purpose
     * @param range interval spec
     * @see IndexedIntSet#parseInterval(String)
     */
    public record Range(String name, String range) {};

    /**
     * Creates the configuration for this provider.
     */
    public static class RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig {
        private final HostPort bootstrapAddress;
        private final String nodeAddressPattern;
        private final int nodeStartPort;
        private final IndexedIntSet nodeIdIndex;

        public RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(@JsonProperty(required = true) HostPort bootstrapAddress,
                                                                              @JsonProperty(required = false) String nodeAddressPattern,
                                                                              @JsonProperty(required = false) Integer nodeStartPort,
                                                                              @JsonProperty(required = true) List<Range> nodeIdRanges) {
            Objects.requireNonNull(bootstrapAddress, "bootstrapAddress cannot be null");
            if (nodeIdRanges.isEmpty()) {
                throw new IllegalArgumentException("node id ranges empty");
            }
            this.bootstrapAddress = bootstrapAddress;
            this.nodeAddressPattern = nodeAddressPattern != null ? nodeAddressPattern : bootstrapAddress.host();
            this.nodeStartPort = nodeStartPort != null ? nodeStartPort : (bootstrapAddress.port() + 1);
            Stream<IndexedIntSet> rangeSets = nodeIdRanges.stream().map(range -> IndexedIntSet.parseInterval(range.range));
            nodeIdIndex = IndexedIntSet.distinctUnion(rangeSets.toList());

            if (this.nodeAddressPattern.isBlank()) {
                throw new IllegalArgumentException("nodeAddressPattern cannot be blank");
            }

            validatePortSpecifier(this.nodeAddressPattern, s -> {
                throw new IllegalArgumentException("nodeAddressPattern cannot have port specifier.  Found port : " + s + " within " + this.nodeAddressPattern);
            });

            if (this.nodeStartPort < 1) {
                throw new IllegalArgumentException("nodeStartPort cannot be less than 1");
            }
            int numberOfNodePorts = nodeIdIndex.size();
            IntStream.range(this.nodeStartPort, this.nodeStartPort + numberOfNodePorts).filter(i -> i == bootstrapAddress.port()).findFirst().ifPresent(i -> {
                throw new IllegalArgumentException("the port used by the bootstrap address (%d) collides with the node port range".formatted(bootstrapAddress.port()));
            });

            validateStringContainsOnlyExpectedTokens(this.nodeAddressPattern, EXPECTED_TOKEN_SET, (token) -> {
                throw new IllegalArgumentException("nodeAddressPattern contains an unexpected replacement token '" + token + "'");
            });
        }

        public HostPort getBootstrapAddress() {
            return bootstrapAddress;
        }
    }

}
