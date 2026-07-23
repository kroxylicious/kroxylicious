/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.internal.net.AddressingSpec;
import io.kroxylicious.proxy.internal.net.AdvertisingSpec;
import io.kroxylicious.proxy.internal.net.BindingSpec;
import io.kroxylicious.proxy.internal.net.ProxyNodeId;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.NodeIdentificationStrategy;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.config.BrokerAddressPatternUtils.LITERAL_NODE_ID;
import static io.kroxylicious.proxy.config.BrokerAddressPatternUtils.validatePortSpecifier;
import static io.kroxylicious.proxy.config.BrokerAddressPatternUtils.validateStringContainsOnlyExpectedTokens;

/**
 * A NodeIdentificationStrategy implementation that uses a separate port per broker endpoint and that is aware of
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
 */
public class PortIdentifiesNodeIdentificationStrategy
        implements NodeIdentificationStrategyFactory {

    private static final Set<String> ALLOWED_TOKEN_SET = Set.of(LITERAL_NODE_ID);

    static final NamedRange DEFAULT_RANGE = new NamedRange("default", 0, 2);

    @JsonProperty(required = true)
    private final HostPort bootstrapAddress;

    // this field is for serialized/deserialization fidelity
    @Nullable
    private final String advertisedBrokerAddressPattern;

    @JsonIgnore
    private final String computedAdvertisedBrokerAddressPattern;

    // this field is for serialized/deserialization fidelity
    @Nullable
    private final Integer nodeStartPort;

    @JsonIgnore
    private final Integer computedNodeStartPort;

    // this field is for serialized/deserialization fidelity
    @Nullable
    private final List<NamedRange> nodeIdRanges;

    @JsonIgnore
    private final List<NamedRange> computedNodeIdRanges;

    @JsonIgnore
    private final Map<Integer, Integer> nodeIdToPort;

    @JsonIgnore
    private final Map<Integer, Integer> portToNodeId;

    @JsonIgnore
    private final Set<Integer> exclusivePorts;

    @JsonCreator
    public PortIdentifiesNodeIdentificationStrategy(@JsonProperty(required = true, value = "bootstrapAddress") HostPort bootstrapAddress,
                                                    @Nullable @JsonProperty(required = false, value = "advertisedBrokerAddressPattern") String advertisedBrokerAddressPattern,
                                                    @Nullable @JsonProperty(required = false, value = "nodeStartPort") Integer nodeStartPort,
                                                    @Nullable @JsonProperty(required = false, value = "nodeIdRanges") List<NamedRange> nodeIdRanges) {
        Objects.requireNonNull(bootstrapAddress, "bootstrapAddress cannot be null");
        this.bootstrapAddress = bootstrapAddress;
        this.advertisedBrokerAddressPattern = advertisedBrokerAddressPattern;
        this.computedAdvertisedBrokerAddressPattern = advertisedBrokerAddressPattern != null ? advertisedBrokerAddressPattern : bootstrapAddress.host();
        verifyNodeAddressPattern(this.computedAdvertisedBrokerAddressPattern);
        this.nodeStartPort = nodeStartPort;
        this.computedNodeStartPort = computeNodeStartPort(bootstrapAddress, nodeStartPort);
        if (this.computedNodeStartPort < 0) {
            throw new IllegalArgumentException("nodeStartPort cannot be negative");
        }
        this.nodeIdRanges = nodeIdRanges;
        var namedRanges = Optional.ofNullable(nodeIdRanges)
                .filter(Predicate.not(List::isEmpty))
                .orElse(List.of(DEFAULT_RANGE));
        verifyRangeNamesAreUnique(namedRanges);
        verifyRangesAreDistinct(namedRanges);
        nodeIdToPort = mapNodeIdToPort(namedRanges, this.computedNodeStartPort);
        portToNodeId = nodeIdToPort.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey, (a, b) -> a));
        int numberOfNodePorts = nodeIdToPort.size();
        if (this.computedNodeStartPort + numberOfNodePorts - 1 > 65535) {
            throw new IllegalArgumentException("The maximum port mapped exceeded 65535");
        }
        if (!isOsAssigned(bootstrapAddress.port())) {
            verifyNoRangeContainsBootstrapPort(bootstrapAddress, namedRanges, this.computedNodeStartPort, nodeIdToPort);
        }
        this.computedNodeIdRanges = namedRanges;
        this.exclusivePorts = Collections.unmodifiableSet(computeExclusivePorts(bootstrapAddress));
    }

    private Set<Integer> computeExclusivePorts(HostPort bootstrapAddress) {
        var ports = nodeIdToPort.values().stream()
                .filter(p -> !isOsAssigned(p))
                .collect(Collectors.toCollection(HashSet::new));
        if (!isOsAssigned(bootstrapAddress.port())) {
            ports.add(bootstrapAddress.port());
        }
        return ports;
    }

    private static int computeNodeStartPort(HostPort bootstrapAddress, @Nullable Integer nodeStartPort) {
        return Objects.requireNonNullElseGet(nodeStartPort, () -> isOsAssigned(bootstrapAddress.port()) ? 0 : bootstrapAddress.port() + 1);
    }

    private static void verifyNodeAddressPattern(String advertisedBrokerAddressPattern) {
        if (advertisedBrokerAddressPattern.isBlank()) {
            throw new IllegalArgumentException("nodeAddressPattern cannot be blank");
        }
        validatePortSpecifier(advertisedBrokerAddressPattern, s -> {
            throw new IllegalArgumentException("nodeAddressPattern cannot have port specifier.  Found port : " + s + " within " + advertisedBrokerAddressPattern);
        });
        validateStringContainsOnlyExpectedTokens(advertisedBrokerAddressPattern, ALLOWED_TOKEN_SET, token -> {
            throw new IllegalArgumentException("nodeAddressPattern contains an unexpected replacement token '" + token + "'");
        });
    }

    private static void verifyRangeNamesAreUnique(List<NamedRange> namedRanges) {
        Map<String, List<NamedRange>> collect = namedRanges.stream().collect(Collectors.groupingBy(NamedRange::name));
        List<String> nonUniqueNames = collect.entrySet().stream().filter(stringListEntry -> stringListEntry.getValue().size() > 1).map(Map.Entry::getKey).toList();
        if (!nonUniqueNames.isEmpty()) {
            throw new IllegalArgumentException("non-unique nodeIdRange names discovered: " + nonUniqueNames);
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
                    + collisions.stream().map(
                            RangeCollision::toString).collect(Collectors.joining(", ")));
        }
    }

    private static void verifyNoRangeContainsBootstrapPort(HostPort bootstrapAddress, List<NamedRange> namedRanges, Integer nodeStartPort1,
                                                           Map<Integer, Integer> nodeIdToPort) {
        for (NamedRange namedRange : namedRanges) {
            namedRange.values().forEach(value -> {
                if (Objects.equals(nodeIdToPort.get(value), bootstrapAddress.port())) {
                    int endExclusive = namedRange.end() + nodeStartPort1 + 1;
                    var portRange = new Range(namedRange.start() + nodeStartPort1, endExclusive);
                    throw new IllegalArgumentException(
                            "the port used by the bootstrap address (%d) collides with the node id range: %s mapped to ports %s".formatted(bootstrapAddress.port(),
                                    namedRange.name() + ":" + namedRange.toIntervalNotationString(), portRange));
                }
            });
        }
    }

    private record RangeCollision(NamedRange a, NamedRange b) {
        @Override
        public String toString() {
            return "'" + a.name() + ":" + a.toIntervalNotationString() + "' collides with '" + b.name() + ":" + b.toIntervalNotationString() + "'";
        }
    }

    private static Map<Integer, Integer> mapNodeIdToPort(List<NamedRange> ranges, Integer nodeStartPort) {
        IntStream unsortedNodeIds = ranges.stream().flatMapToInt(NamedRange::values);
        List<Integer> ascendingNodeIds = unsortedNodeIds.distinct().sorted().boxed().toList();
        Map<Integer, Integer> nodeIdToPort = new HashMap<>();
        for (int offset = 0; offset < ascendingNodeIds.size(); offset++) {
            nodeIdToPort.put(ascendingNodeIds.get(offset), isOsAssigned(nodeStartPort) ? 0 : nodeStartPort + offset);
        }
        return nodeIdToPort;
    }

    /**
     * A port of 0 is the sentinel meaning "let the OS assign an ephemeral port at bind time"
     * rather than a fixed port number.
     */
    private static boolean isOsAssigned(int port) {
        return port == 0;
    }

    @Nullable
    @JsonProperty
    public Integer getNodeStartPort() {
        return nodeStartPort;
    }

    @Nullable
    @JsonProperty
    public List<NamedRange> getNodeIdRanges() {
        return nodeIdRanges;
    }

    @Nullable
    @JsonProperty
    public String getAdvertisedBrokerAddressPattern() {
        return advertisedBrokerAddressPattern;
    }

    @JsonProperty(required = true)
    public HostPort getBootstrapAddress() {
        return bootstrapAddress;
    }

    @Override
    public NodeIdentificationStrategy buildStrategy(String clusterName) {
        return new Strategy();
    }

    @Override
    @SuppressWarnings("EqualsGetClass") // Distinct NodeIdentificationStrategyFactory implementations must never compare equal. getClass()
    // enforces that; instanceof would not if this class were ever subclassed.
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (PortIdentifiesNodeIdentificationStrategy) obj;
        return Objects.equals(this.bootstrapAddress, that.bootstrapAddress) &&
                Objects.equals(this.computedAdvertisedBrokerAddressPattern, that.computedAdvertisedBrokerAddressPattern) &&
                Objects.equals(this.computedNodeStartPort, that.computedNodeStartPort) &&
                Objects.equals(this.computedNodeIdRanges, that.computedNodeIdRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bootstrapAddress, computedAdvertisedBrokerAddressPattern, computedNodeStartPort, computedNodeIdRanges);
    }

    @Override
    public String toString() {
        return "PortIdentifiesNodeIdentificationStrategy[" +
                "bootstrapAddress=" + bootstrapAddress + ", " +
                "advertisedBrokerAddressPattern=" + computedAdvertisedBrokerAddressPattern + ", " +
                "nodeStartPort=" + computedNodeStartPort + ", " +
                "nodeIdRanges=" + computedNodeIdRanges + ']';
    }

    private class Strategy implements NodeIdentificationStrategy, BindingSpec, AdvertisingSpec, AddressingSpec {

        private final Map<Integer, Integer> boundPortToNodeId = new ConcurrentHashMap<>();
        private volatile int boundBootstrapPort = -1;

        @Override
        public HostPort getClusterBootstrapAddress() {
            return bootstrapAddress;
        }

        @Override
        public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
            if (!nodeIdToPort.containsKey(nodeId)) {
                throw new IllegalArgumentException(
                        "Cannot generate node address for node id %d as it is not contained in the ranges defined for provider with downstream bootstrap %s"
                                .formatted(nodeId, bootstrapAddress));
            }
            int port = nodeIdToPort.get(nodeId);
            return new HostPort(BrokerAddressPatternUtils.replaceLiteralNodeId(computedAdvertisedBrokerAddressPattern, nodeId), port);
        }

        @Override
        public Set<Integer> getExclusivePorts() {
            return exclusivePorts;
        }

        @Override
        public Map<Integer, HostPort> discoveryAddressMap() {
            return nodeIdToPort.keySet().stream()
                    .collect(Collectors.toMap(Function.identity(), this::getBrokerAddress));
        }

        @Override
        public HostPort getBootstrapBindAddress() {
            return bootstrapAddress;
        }

        @Override
        public Map<Integer, HostPort> nodeBindAddresses() {
            return nodeIdToPort.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> new HostPort(bootstrapAddress.host(), e.getValue())));
        }

        @Override
        public Optional<String> getBindAddress() {
            return Optional.empty();
        }

        @Override
        public Set<Integer> getSharedPorts() {
            return Set.of();
        }

        @Override
        public boolean requiresServerNameIndication() {
            return false;
        }

        @Override
        public void registerBoundPort(int nodeId, int actualPort) {
            boundPortToNodeId.put(actualPort, nodeId);
        }

        @Override
        public void registerBoundBootstrapPort(int actualPort) {
            boundBootstrapPort = actualPort;
        }

        @Override
        public HostPort advertisedBootstrapAddress(ProxyNodeId.Bootstrap bootstrap) {
            return new HostPort(bootstrapAddress.host(), bootstrap.gateway().resolvePort(bootstrap));
        }

        @Override
        public HostPort advertisedBrokerAddress(ProxyNodeId virtualNodeId) throws IllegalArgumentException {
            int nodeId = ((ProxyNodeId.Broker) virtualNodeId).nodeId();
            String host = BrokerAddressPatternUtils.replaceLiteralNodeId(computedAdvertisedBrokerAddressPattern, nodeId);
            return new HostPort(host, virtualNodeId.gateway().resolvePort(virtualNodeId));
        }

        @Override
        public Target identify(int port, @Nullable String sniHostname) {
            if (port == bootstrapAddress.port() || port == boundBootstrapPort) {
                return new Target.Bootstrap();
            }
            Integer nodeId = boundPortToNodeId.get(port);
            if (nodeId == null) {
                nodeId = portToNodeId.get(port);
            }
            return nodeId != null ? new Target.Node(nodeId) : new Target.NotRecognised();
        }

    }

}
