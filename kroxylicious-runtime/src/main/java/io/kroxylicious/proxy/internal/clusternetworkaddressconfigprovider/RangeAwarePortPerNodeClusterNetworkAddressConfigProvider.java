/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProviderService;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.EXPECTED_TOKEN_SET;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validatePortSpecifier;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.BrokerAddressPatternUtils.validateStringContainsOnlyExpectedTokens;

/**
 * A ClusterNetworkAddressConfigProvider implementation that uses a separate port per broker endpoint and that is aware of
 * distinct ranges of nodeIds present in the target cluster. Upstream nodeIds are mapped to a compact set of ports.
 * <br/>
 * The following configuration is supported:
 * <ul>
 *    <li>{@code bootstrapAddress} (required) a {@link HostPort} defining the host and port of the bootstrap address.</li>
 *    <li>{@code brokerAddressPattern} (optional) an address pattern used to form broker addresses.  It is addresses made from this pattern that are returned to the kafka
 *    client in the Metadata response so must be resolvable by the client.  One pattern is supported: {@code $(nodeId)} which interpolates the node id into the address.
 *    If brokerAddressPattern is omitted, it defaulted it based on the host name of {@code bootstrapAddress}.</li>
 *    <li>{@code brokerStartPort} (optional) defines the starting range of port number that will be assigned to the brokers.  If omitted, it is defaulted to
 *    the port number of {@code bootstrapAddress + 1}.</li>
 *    <li>{@code nodeIdRanges} (required) defines the node id ranges present in the target cluster</li>
 * </ul>
 *
 * @deprecated use {@link io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy} instead
 */
@Plugin(configType = RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig.class)
@Deprecated(since = "0.11.0", forRemoval = true)
public class RangeAwarePortPerNodeClusterNetworkAddressConfigProvider implements
        ClusterNetworkAddressConfigProviderService<RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig> {

    @NonNull
    @Override
    public ClusterNetworkAddressConfigProvider build(RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig config) {
        return new Provider(config);
    }

    private static class Provider implements ClusterNetworkAddressConfigProvider {
        private final HostPort bootstrapAddress;
        private final String nodeAddressPattern;
        private final Set<Integer> exclusivePorts;
        private final Map<Integer, Integer> nodeIdToPort;

        /**
         * Creates the provider.
         *
         * @param config configuration
         */
        private Provider(RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig config) {
            this.bootstrapAddress = config.bootstrapAddress;
            this.nodeAddressPattern = config.nodeAddressPattern;
            this.nodeIdToPort = config.nodeIdToPort;
            var allExclusivePorts = new HashSet<>(nodeIdToPort.values());
            allExclusivePorts.add(bootstrapAddress.port());
            this.exclusivePorts = Collections.unmodifiableSet(allExclusivePorts);
        }

        @Override
        public HostPort getClusterBootstrapAddress() {
            return this.bootstrapAddress;
        }

        @Override
        public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
            if (!nodeIdToPort.containsKey(nodeId)) {
                throw new IllegalArgumentException(
                        "Cannot generate node address for node id %d as it is not contained in the ranges defined for provider with downstream bootstrap %s"
                                .formatted(
                                        nodeId,
                                        bootstrapAddress));
            }
            int port = nodeIdToPort.get(nodeId);
            return new HostPort(BrokerAddressPatternUtils.replaceLiteralNodeId(nodeAddressPattern, nodeId), port);
        }

        @Override
        public Set<Integer> getExclusivePorts() {
            return this.exclusivePorts;
        }

        @Override
        public Map<Integer, HostPort> discoveryAddressMap() {
            return nodeIdToPort.keySet().stream()
                    .collect(Collectors.toMap(Function.identity(), this::getBrokerAddress));
        }

    }

    /**
     * Creates the configuration for this provider.
     */
    public static class RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig {
        private final HostPort bootstrapAddress;
        private final String nodeAddressPattern;
        private final int nodeStartPort;
        @JsonIgnore
        private final Map<Integer, Integer> nodeIdToPort;

        @SuppressWarnings("java:S1068") // included so Jackson can serialize/deserialize this with fidelity
        private final List<NamedRangeSpec> nodeIdRanges;

        public RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(@JsonProperty(required = true) HostPort bootstrapAddress,
                                                                              @JsonProperty(required = false) String nodeAddressPattern,
                                                                              @JsonProperty(required = false) Integer nodeStartPort,
                                                                              @JsonProperty(required = true) List<NamedRangeSpec> nodeIdRanges) {
            Objects.requireNonNull(bootstrapAddress, "bootstrapAddress cannot be null");
            if (nodeIdRanges.isEmpty()) {
                throw new IllegalArgumentException("node id ranges empty");
            }
            this.bootstrapAddress = bootstrapAddress;
            this.nodeAddressPattern = nodeAddressPattern != null ? nodeAddressPattern : bootstrapAddress.host();
            verifyNodeAddressPattern();
            this.nodeStartPort = nodeStartPort != null ? nodeStartPort : (bootstrapAddress.port() + 1);
            if (this.nodeStartPort < 1) {
                throw new IllegalArgumentException("nodeStartPort cannot be less than 1");
            }
            List<NamedRange> namedRanges = nodeIdRanges.stream().map(NamedRangeSpec::range).toList();
            verifyRangeNamesAreUnique(namedRanges);
            verifyRangesAreDistinct(namedRanges);
            nodeIdToPort = mapNodeIdToPort(namedRanges, this.nodeStartPort);
            int numberOfNodePorts = nodeIdToPort.size();
            if (this.nodeStartPort + numberOfNodePorts - 1 > 65535) {
                throw new IllegalArgumentException("The maximum port mapped exceeded 65535");
            }
            verifyNoRangeContainsBootstrapPort(bootstrapAddress, namedRanges);
            this.nodeIdRanges = nodeIdRanges;
        }

        private void verifyNoRangeContainsBootstrapPort(HostPort bootstrapAddress, List<NamedRange> namedRanges) {
            for (NamedRange namedRange : namedRanges) {
                namedRange.range().values().forEach(value -> {
                    if (Objects.equals(this.nodeIdToPort.get(value), this.bootstrapAddress.port())) {
                        Range range = namedRange.range;
                        var portRange = new Range(range.startInclusive() + this.nodeStartPort, range.endExclusive() + this.nodeStartPort);
                        throw new IllegalArgumentException(
                                "the port used by the bootstrap address (%d) collides with the node id range: %s mapped to ports %s".formatted(bootstrapAddress.port(),
                                        namedRange, portRange));
                    }
                });
            }
        }

        private void verifyNodeAddressPattern() {
            if (this.nodeAddressPattern.isBlank()) {
                throw new IllegalArgumentException("nodeAddressPattern cannot be blank");
            }
            validatePortSpecifier(this.nodeAddressPattern, s -> {
                throw new IllegalArgumentException("nodeAddressPattern cannot have port specifier.  Found port : " + s + " within " + this.nodeAddressPattern);
            });
            validateStringContainsOnlyExpectedTokens(this.nodeAddressPattern, EXPECTED_TOKEN_SET, token -> {
                throw new IllegalArgumentException("nodeAddressPattern contains an unexpected replacement token '" + token + "'");
            });
        }

        private static void verifyRangeNamesAreUnique(List<NamedRange> namedRanges) {
            Map<String, List<NamedRange>> collect = namedRanges.stream().collect(Collectors.groupingBy(namedRange -> namedRange.name));
            List<String> nonUniqueNames = collect.entrySet().stream().filter(stringListEntry -> stringListEntry.getValue().size() > 1).map(Map.Entry::getKey).toList();
            if (!nonUniqueNames.isEmpty()) {
                throw new IllegalArgumentException("non-unique nodeIdRange names discovered: " + nonUniqueNames);
            }
        }

        private static Map<Integer, Integer> mapNodeIdToPort(List<NamedRange> ranges, Integer nodeStartPort) {
            IntStream unsortedNodeIds = ranges.stream().flatMapToInt(rangeSpec -> rangeSpec.range().values());
            List<Integer> ascendingNodeIds = unsortedNodeIds.distinct().sorted().boxed().toList();
            Map<Integer, Integer> nodeIdToPort = new HashMap<>();
            for (int offset = 0; offset < ascendingNodeIds.size(); offset++) {
                nodeIdToPort.put(ascendingNodeIds.get(offset), nodeStartPort + offset);
            }
            return nodeIdToPort;
        }

        public HostPort getBootstrapAddress() {
            return bootstrapAddress;
        }

        public String getNodeAddressPattern() {
            return nodeAddressPattern;
        }

        public int getNodeStartPort() {
            return nodeStartPort;
        }

        public List<NamedRangeSpec> getNodeIdRanges() {
            return nodeIdRanges;
        }

        private record RangeCollision(NamedRange a, NamedRange b) {
            @Override
            public String toString() {
                return "'" + a + "' collides with '" + b + "'";
            }
        }

        private static void verifyRangesAreDistinct(List<NamedRange> ranges) {
            Collection<RangeCollision> collisions = new ArrayList<>();
            for (int i = 0; i < ranges.size(); i++) {
                for (int j = 0; j < ranges.size(); j++) {
                    // this is to compare unique, non-identical indices only once. ie we compare 2,3 but not 3,2
                    if (j > i) {
                        NamedRange rangeA = ranges.get(i);
                        NamedRange rangeB = ranges.get(j);
                        if (!rangeA.isDistinctFrom(rangeB)) {
                            collisions.add(new RangeCollision(rangeA, rangeB));
                        }
                    }
                }
            }
            if (!collisions.isEmpty()) {
                throw new IllegalArgumentException("some nodeIdRanges collided (one or more node ids are duplicated in the following ranges): "
                        + collisions.stream().map(RangeCollision::toString).collect(Collectors.joining(", ")));
            }
        }
    }

    /**
     * @param startInclusive the (inclusive) initial value
     * @param endExclusive the exclusive upper bound
     */
    public record IntRangeSpec(@JsonInclude(JsonInclude.Include.ALWAYS) @JsonProperty(required = true) int startInclusive,
                               @JsonInclude(JsonInclude.Include.ALWAYS) @JsonProperty(required = true) int endExclusive) {

        public Range range() {
            return new Range(startInclusive, endExclusive);
        }
    }

    private record NamedRange(@NonNull String name, @NonNull Range range) {
        public NamedRange {
            Objects.requireNonNull(name, "name was null");
            Objects.requireNonNull(range, "range was null");
        }

        public boolean isDistinctFrom(NamedRange rangeB) {
            return range.isDistinctFrom(rangeB.range);
        }

        @Override
        public String toString() {
            return name + ":" + range;
        }
    }

    /**
     * @param name the name of this range
     * @param rangeSpec specification of the range
     */
    public record NamedRangeSpec(@JsonProperty(required = true) String name,
                                 @JsonProperty(required = true, value = "range") IntRangeSpec rangeSpec) {

        NamedRange range() {
            return new NamedRange(name, tryBuildRange());
        }

        private Range tryBuildRange() {
            try {
                return rangeSpec.range();
            }
            catch (Exception e) {
                throw new IllegalArgumentException("invalid nodeIdRange: " + name + ", " + e.getMessage(), e);
            }
        }

        @Override
        public String toString() {
            return name + ":" + rangeSpec.range();
        }
    }

}
