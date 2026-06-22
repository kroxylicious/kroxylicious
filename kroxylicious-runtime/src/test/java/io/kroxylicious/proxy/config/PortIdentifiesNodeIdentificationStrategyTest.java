/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.internal.net.AdvertisingSpec;
import io.kroxylicious.proxy.internal.net.BindingSpec;
import io.kroxylicious.proxy.internal.net.RoutingSpec;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.NodeIdentificationStrategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PortIdentifiesNodeIdentificationStrategyTest {

    private static final String BOOTSTRAP_HOST = "cluster.kafka.example.com";
    private static final String BOOTSTRAP = BOOTSTRAP_HOST + ":1235";
    private static final HostPort BOOSTRAP_HOSTPORT = HostPort.parse(BOOTSTRAP);
    public static final String ADVERTISED_BROKER_ADDRESS_PATTERN = "broker$(nodeId).kafka.example.com";

    @Test
    void brokerAddressSingleRange() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null,
                List.of(new NamedRange("brokers", 0, 2))).buildStrategy("cluster");
        assertThat(strategy.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 1236));
        assertThat(strategy.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 1237));
    }

    @Test
    void brokerAddressDefaultRange() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null,
                null).buildStrategy("cluster");
        assertThat(strategy.getExclusivePorts()).hasSize(4);
        assertThat(strategy.getClusterBootstrapAddress()).isEqualTo(BOOSTRAP_HOSTPORT);
        assertThat(strategy.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 1236));
        assertThat(strategy.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 1237));
        assertThat(strategy.getBrokerAddress(2)).isEqualTo(new HostPort("broker2.kafka.example.com", 1238));
    }

    @Test
    void brokerAddressInferredFromBootstrapIfNotExplicitlySupplied() {
        List<NamedRange> namedRangeSpecs = List.of(new NamedRange("brokers", 0, 2));
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                null, null,
                namedRangeSpecs).buildStrategy("cluster");
        assertThat(strategy.getBrokerAddress(0)).isEqualTo(new HostPort(BOOTSTRAP_HOST, 1236));
        assertThat(strategy.getBrokerAddress(1)).isEqualTo(new HostPort(BOOTSTRAP_HOST, 1237));
    }

    @Test
    void nodeAddressPatternCannotBeBlank() {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "", null,
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeAddressPattern cannot be blank");
    }

    @Test
    void nodeAddressPatternCannotContainUnexpectedReplacementPatterns() {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "node-$(typoedNodeId).broker.com", null,
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeAddressPattern contains an unexpected replacement token '$(typoedNodeId)'");
    }

    @Test
    void nodeAddressPatternCannotContainVirtualClusterNameReplacementPattern() {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "node-$(virtualClusterName).broker.com", null,
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeAddressPattern contains an unexpected replacement token '$(virtualClusterName)'");
    }

    @Test
    void nodeAddressPatternCannotContainPort() {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "localhost:8080", null,
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeAddressPattern cannot have port specifier.  Found port : 8080 within localhost:8080");
    }

    @Test
    void computedNodeStartPortCannotBeNegative() {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "localhost", -1,
                null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldDefaultAllNodePortsToZeroWhenBootstrapIsOsAssigned() {
        // Given - bootstrap on port 0 (OS-assigned), no nodeStartPort specified
        var bootstrap = HostPort.parse(BOOTSTRAP_HOST + ":0");

        // When - no exception; node ports default to 0 (OS-assigned) rather than 1 (bootstrap+1)
        var spec = (BindingSpec) new PortIdentifiesNodeIdentificationStrategy(bootstrap, null, null, null)
                .buildStrategy("cluster");

        // Then - all node bind addresses use port 0 (OS-assigned)
        assertThat(spec.nodeBindAddresses().values()).allSatisfy(hp -> assertThat(hp.port()).isEqualTo(0));
    }

    @Test
    void shouldMapAllNodesToPortZeroWhenNodeStartPortIsZero() {
        // Given - bootstrap on port 0, nodeStartPort explicitly 0
        var bootstrap = HostPort.parse(BOOTSTRAP_HOST + ":0");
        var spec = (BindingSpec) new PortIdentifiesNodeIdentificationStrategy(bootstrap,
                ADVERTISED_BROKER_ADDRESS_PATTERN, 0,
                List.of(new NamedRange("brokers", 0, 2))).buildStrategy("cluster");

        // When
        Map<Integer, HostPort> addresses = spec.nodeBindAddresses();

        // Then - all nodes map to port 0 (OS-assigned independently)
        assertThat(addresses).containsOnlyKeys(0, 1, 2);
        assertThat(addresses.get(0).port()).isEqualTo(0);
        assertThat(addresses.get(1).port()).isEqualTo(0);
        assertThat(addresses.get(2).port()).isEqualTo(0);
    }

    @Test
    void nodePortRangeCannotCollideWithBootstrapPort() {
        List<NamedRange> rangeSpecs = List.of(new NamedRange("brokers", 0, 3));
        HostPort bootstrapAddress = HostPort.parse(BOOTSTRAP_HOST + ":1235");
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(bootstrapAddress,
                "localhost",
                // node id 1 will be assigned port 1235 and collide with bootstrap
                1234,
                rangeSpecs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("the port used by the bootstrap address (1235) collides with the node id range: brokers:[0,3] mapped to ports [1234,1238)");
    }

    @Test
    void getClusterBootstrap() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "broker$(nodeId).kafka.example.com",
                1236,
                null).buildStrategy("cluster");
        assertThat(strategy.getClusterBootstrapAddress()).isEqualTo(BOOSTRAP_HOSTPORT);
    }

    @Test
    void exclusivePortsSingleRange() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                List.of(new NamedRange("brokers", 0, 1))).buildStrategy("cluster");
        assertThat(strategy.getExclusivePorts()).containsExactly(1235, 1236, 1237);
    }

    @Test
    void discoveryAddressMapSingleRange() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                List.of(new NamedRange("brokers", 0, 1))).buildStrategy("cluster");
        Map<Integer, HostPort> expected = Map.of(
                0, new HostPort("broker0.kafka.example.com", 1236),
                1, new HostPort("broker1.kafka.example.com", 1237));
        assertThat(strategy.discoveryAddressMap()).isEqualTo(expected);
    }

    @Test
    void brokerAddressMultipleRanges() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                List.of(new NamedRange("brokers", 0, 1), new NamedRange("controllers", 3, 4))).buildStrategy("cluster");
        assertThat(strategy.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 1236));
        assertThat(strategy.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 1237));
        assertThat(strategy.getBrokerAddress(3)).isEqualTo(new HostPort("broker3.kafka.example.com", 1238));
        assertThat(strategy.getBrokerAddress(4)).isEqualTo(new HostPort("broker4.kafka.example.com", 1239));
    }

    @Test
    void brokerAddressUnknownNodeId() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                List.of(new NamedRange("brokers", 0, 1), new NamedRange("controllers", 3, 4))).buildStrategy("cluster");
        String expectedMessage = "Cannot generate node address for node id 5 as it is not contained in the ranges defined for provider with downstream bootstrap "
                + BOOTSTRAP;
        assertThatThrownBy(() -> strategy.getBrokerAddress(5)).isInstanceOf(IllegalArgumentException.class).hasMessage(expectedMessage);
    }

    @Test
    void discoveryAddressMapMultipleRanges() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                List.of(new NamedRange("brokers", 0, 1), new NamedRange("controllers", 3, 4))).buildStrategy("cluster");

        Map<Integer, HostPort> expected = Map.of(
                0, new HostPort("broker0.kafka.example.com", 1236),
                1, new HostPort("broker1.kafka.example.com", 1237),
                3, new HostPort("broker3.kafka.example.com", 1238),
                4, new HostPort("broker4.kafka.example.com", 1239));
        assertThat(strategy.discoveryAddressMap()).isEqualTo(expected);
    }

    static Stream<Arguments> overlappingComputedNodeIdRangesAreInvalid() {
        Arguments twoRangesWithOverlap = Arguments.arguments(
                List.of(new NamedRange("brokers", 0, 1), new NamedRange("controllers", 1, 1)),
                "some nodeIdRanges collided (one or more node ids are duplicated in the following ranges): 'brokers:[0,1]' collides with 'controllers:[1,1]'");
        Arguments threeRangesWithFirstAndLastOverlap = Arguments.arguments(
                List.of(new NamedRange("brokers", 0, 1), new NamedRange("controllers", 7, 7),
                        new NamedRange("other", 1, 1)),
                "some nodeIdRanges collided (one or more node ids are duplicated in the following ranges): 'brokers:[0,1]' collides with 'other:[1,1]'");
        Arguments multipleOverlaps = Arguments.arguments(
                List.of(new NamedRange("brokers", 0, 1), new NamedRange("controllers", 1, 3),
                        new NamedRange("other", 3, 4)),
                "some nodeIdRanges collided (one or more node ids are duplicated in the following ranges): 'brokers:[0,1]' collides with 'controllers:[1,3]', 'controllers:[1,3]' collides with 'other:[3,4]'");
        return Stream.of(twoRangesWithOverlap, threeRangesWithFirstAndLastOverlap, multipleOverlaps);
    }

    @MethodSource
    @ParameterizedTest
    void overlappingComputedNodeIdRangesAreInvalid(List<NamedRange> namedRangeSpecs, String expectedException) {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                namedRangeSpecs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedException);
    }

    @Test
    void rangesMustHaveUniqueNames() {
        List<NamedRange> nodeIdRanges = List.of(new NamedRange("brokers", 0, 1), new NamedRange("brokers", 1, 2));
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                nodeIdRanges))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("non-unique nodeIdRange names discovered: [brokers]");
    }

    @Test
    void exclusivePortsMultipleRanges() {
        List<NamedRange> nodeIdRanges = List.of(new NamedRange("brokers", 0, 1), new NamedRange("controllers", 3, 4));
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                nodeIdRanges).buildStrategy("cluster");
        assertThat(strategy.getExclusivePorts()).containsExactly(1235, 1236, 1237, 1238, 1239);
    }

    @Test
    void allNodeIdsMustBeMappableToAValidPort() {
        List<NamedRange> ranges = List.of(new NamedRange("brokers", 0, 65534));
        HostPort bootstrapAddress = HostPort.parse(BOOTSTRAP_HOST + ":1");
        assertThatThrownBy(() -> {
            new PortIdentifiesNodeIdentificationStrategy(bootstrapAddress,
                    ADVERTISED_BROKER_ADDRESS_PATTERN,
                    null,
                    ranges);
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The maximum port mapped exceeded 65535");
    }

    @Test
    void getBootstrapAddressFromConfig() {
        var bootstrapAddress = HostPort.parse(BOOTSTRAP);
        var config = new PortIdentifiesNodeIdentificationStrategy(bootstrapAddress,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                null);
        assertThat(config.getBootstrapAddress()).isEqualTo(bootstrapAddress);
    }

    @Test
    void canBuildNodeIdentificationStrategy() {
        var bootstrap = HostPort.parse("boot:1234");
        var config = new PortIdentifiesNodeIdentificationStrategy(bootstrap, "mybroker", null, null);
        var strategy = buildNodeIdentificationStrategy(config);
        assertThat(strategy.getClusterBootstrapAddress()).isEqualTo(bootstrap);
    }

    @Test
    void providesDefaultRange() {
        var bootstrap = HostPort.parse("boot:1234");
        var config = new PortIdentifiesNodeIdentificationStrategy(bootstrap, "mybroker", null, null);
        var strategy = buildNodeIdentificationStrategy(config);
        assertThat(strategy.getBrokerAddress(0)).isEqualTo(HostPort.parse("mybroker:1235"));
        assertThat(strategy.getBrokerAddress(1)).isEqualTo(HostPort.parse("mybroker:1236"));
        assertThat(strategy.getBrokerAddress(2)).isEqualTo(HostPort.parse("mybroker:1237"));
        assertThatThrownBy(() -> strategy.getBrokerAddress(3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void respectsSingleRange() {
        var bootstrap = HostPort.parse("boot:1234");
        var config = new PortIdentifiesNodeIdentificationStrategy(bootstrap, "mybroker", null, List.of(new NamedRange("foo", 10, 11)));
        var strategy = buildNodeIdentificationStrategy(config);

        assertThat(strategy.getBrokerAddress(10)).isEqualTo(HostPort.parse("mybroker:1235"));
        assertThat(strategy.getBrokerAddress(11)).isEqualTo(HostPort.parse("mybroker:1236"));

        assertThatThrownBy(() -> strategy.getBrokerAddress(9))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strategy.getBrokerAddress(12))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void respectsStartPort() {
        var bootstrap = HostPort.parse("boot:1234");
        var config = new PortIdentifiesNodeIdentificationStrategy(bootstrap, "mybroker", 1240, null);
        var strategy = buildNodeIdentificationStrategy(config);
        assertThat(strategy.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("mybroker:1240"));
    }

    private NodeIdentificationStrategy buildNodeIdentificationStrategy(PortIdentifiesNodeIdentificationStrategy config) {
        return config.buildStrategy("cluster");
    }

    // ---- AdvertisingSpec ----

    @Test
    void advertisedBrokerHostReturnsHostnameOnlyNoPort() {
        // Given
        var spec = (AdvertisingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null,
                List.of(new NamedRange("brokers", 0, 1))).buildStrategy("cluster");

        // When
        String host = spec.getAdvertisedBrokerHost(0);

        // Then — returned value is a plain hostname, not a host:port string
        assertThat(host).isEqualTo("broker0.kafka.example.com");
        assertThat(host).doesNotContain(":");
    }

    @Test
    void advertisedBrokerHostExpandsNodeIdInPattern() {
        // Given
        var spec = (AdvertisingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null,
                List.of(new NamedRange("brokers", 0, 2))).buildStrategy("cluster");

        // When / Then
        assertThat(spec.getAdvertisedBrokerHost(0)).isEqualTo("broker0.kafka.example.com");
        assertThat(spec.getAdvertisedBrokerHost(1)).isEqualTo("broker1.kafka.example.com");
        assertThat(spec.getAdvertisedBrokerHost(2)).isEqualTo("broker2.kafka.example.com");
    }

    @Test
    void advertisedBrokerHostWithZeroBootstrapPortStillReturnsHostOnly() {
        // Given — bootstrap on port 0 (OS-assigned); advertising must not embed port=0 as a sentinel
        var bootstrap = HostPort.parse(BOOTSTRAP_HOST + ":0");
        var spec = (AdvertisingSpec) new PortIdentifiesNodeIdentificationStrategy(bootstrap,
                ADVERTISED_BROKER_ADDRESS_PATTERN, 1,
                List.of(new NamedRange("brokers", 0, 0))).buildStrategy("cluster");

        // When
        String host = spec.getAdvertisedBrokerHost(0);

        // Then — hostname is returned, the zero-port problem cannot arise
        assertThat(host).isEqualTo("broker0.kafka.example.com");
    }

    @Test
    void advertisedBootstrapHostReturnsBootstrapHost() {
        // Given
        var spec = (AdvertisingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null, null).buildStrategy("cluster");

        // When
        String host = spec.getAdvertisedBootstrapHost();

        // Then
        assertThat(host).isEqualTo(BOOTSTRAP_HOST);
    }

    // ---- RoutingSpec ----

    @Test
    void identifyReturnsEmptyForBootstrapPort() {
        // Given
        var spec = (RoutingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null,
                List.of(new NamedRange("brokers", 0, 1))).buildStrategy("cluster");

        // When
        var result = spec.identify(BOOSTRAP_HOSTPORT.port(), null);

        // Then — empty signals a bootstrap connection (no specific node)
        assertThat(result).isEmpty();
    }

    @Test
    void identifyReturnsMappedNodeIdForNodePort() {
        // Given
        var spec = (RoutingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null,
                List.of(new NamedRange("brokers", 0, 2))).buildStrategy("cluster");
        int node0Port = BOOSTRAP_HOSTPORT.port() + 1;
        int node1Port = BOOSTRAP_HOSTPORT.port() + 2;
        int node2Port = BOOSTRAP_HOSTPORT.port() + 3;

        // When / Then — each node port maps to the correct nodeId
        assertThat(spec.identify(node0Port, null)).contains(0);
        assertThat(spec.identify(node1Port, null)).contains(1);
        assertThat(spec.identify(node2Port, null)).contains(2);
    }

    @Test
    void identifyIgnoresSniForPortBasedRouting() {
        // Given
        var spec = (RoutingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null,
                List.of(new NamedRange("brokers", 0, 0))).buildStrategy("cluster");
        int node0Port = BOOSTRAP_HOSTPORT.port() + 1;

        // When — SNI is present but irrelevant for port-identifies-node routing
        var result = spec.identify(node0Port, "some.sni.hostname.example.com");

        // Then — nodeId is still correctly identified from port
        assertThat(result).contains(0);
    }

    @Test
    void identifyReturnsEmptyForBootstrapPortEvenWithSni() {
        // Given
        var spec = (RoutingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null, null).buildStrategy("cluster");

        // When
        var result = spec.identify(BOOSTRAP_HOSTPORT.port(), "bootstrap.example.com");

        // Then
        assertThat(result).isEmpty();
    }

    // ---- BindingSpec ----

    @Test
    void bootstrapBindAddressMatchesConfiguredBootstrap() {
        // Given
        var spec = (BindingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null, null).buildStrategy("cluster");

        // When
        HostPort addr = spec.getBootstrapBindAddress();

        // Then
        assertThat(addr).isEqualTo(BOOSTRAP_HOSTPORT);
    }

    @Test
    void nodeBindAddressesContainsAllConfiguredNodes() {
        // Given
        var spec = (BindingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null,
                List.of(new NamedRange("brokers", 0, 2))).buildStrategy("cluster");

        // When
        Map<Integer, HostPort> addresses = spec.nodeBindAddresses();

        // Then — all three nodeIds are present with their assigned ports
        assertThat(addresses).containsOnlyKeys(0, 1, 2);
        assertThat(addresses.get(0).port()).isEqualTo(BOOSTRAP_HOSTPORT.port() + 1);
        assertThat(addresses.get(1).port()).isEqualTo(BOOSTRAP_HOSTPORT.port() + 2);
        assertThat(addresses.get(2).port()).isEqualTo(BOOSTRAP_HOSTPORT.port() + 3);
    }

    @Test
    void bindingSpecDoesNotRequireServerNameIndication() {
        // Given
        var spec = (BindingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null, null).buildStrategy("cluster");

        // When / Then
        assertThat(spec.requiresServerNameIndication()).isFalse();
    }

    @Test
    void bindingSpecHasNoSharedPorts() {
        // Given
        var spec = (BindingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null, null).buildStrategy("cluster");

        // When / Then
        assertThat(spec.getSharedPorts()).isEmpty();
    }

    @Test
    void bindingSpecBindsOnAllInterfaces() {
        // Given
        var spec = (BindingSpec) new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null, null).buildStrategy("cluster");

        // When / Then — empty means bind on all interfaces (0.0.0.0)
        assertThat(spec.getBindAddress()).isEmpty();
    }

}
