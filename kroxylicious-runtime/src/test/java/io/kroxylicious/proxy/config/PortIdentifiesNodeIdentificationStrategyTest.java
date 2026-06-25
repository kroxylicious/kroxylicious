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
    void shouldReturnBrokerAddressForSingleRange() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN, null,
                List.of(new NamedRange("brokers", 0, 2))).buildStrategy("cluster");
        assertThat(strategy.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 1236));
        assertThat(strategy.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 1237));
    }

    @Test
    void shouldReturnBrokerAddressForDefaultRange() {
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
    void shouldInferAdvertisedBrokerAddressFromBootstrap() {
        List<NamedRange> namedRangeSpecs = List.of(new NamedRange("brokers", 0, 2));
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                null, null,
                namedRangeSpecs).buildStrategy("cluster");
        assertThat(strategy.getBrokerAddress(0)).isEqualTo(new HostPort(BOOTSTRAP_HOST, 1236));
        assertThat(strategy.getBrokerAddress(1)).isEqualTo(new HostPort(BOOTSTRAP_HOST, 1237));
    }

    @Test
    void shouldRejectBlankNodeAddressPattern() {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "", null,
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeAddressPattern cannot be blank");
    }

    @Test
    void shouldRejectNodeAddressPatternWithUnexpectedReplacementTokens() {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "node-$(typoedNodeId).broker.com", null,
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeAddressPattern contains an unexpected replacement token '$(typoedNodeId)'");
    }

    @Test
    void shouldRejectNodeAddressPatternContainingVirtualClusterNameToken() {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "node-$(virtualClusterName).broker.com", null,
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeAddressPattern contains an unexpected replacement token '$(virtualClusterName)'");
    }

    @Test
    void shouldRejectNodeAddressPatternContainingPortSpecifier() {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "localhost:8080", null,
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeAddressPattern cannot have port specifier.  Found port : 8080 within localhost:8080");
    }

    @Test
    void shouldRejectNodeStartPortLessThanOne() {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "localhost", 0,
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeStartPort cannot be less than 1");
    }

    @Test
    void shouldRejectNodePortRangeThatCollidesWithBootstrapPort() {
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
    void shouldReturnConfiguredBootstrapAddress() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                "broker$(nodeId).kafka.example.com",
                1236,
                null).buildStrategy("cluster");
        assertThat(strategy.getClusterBootstrapAddress()).isEqualTo(BOOSTRAP_HOSTPORT);
    }

    @Test
    void shouldReportExclusivePortsForSingleRange() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                List.of(new NamedRange("brokers", 0, 1))).buildStrategy("cluster");
        assertThat(strategy.getExclusivePorts()).containsExactly(1235, 1236, 1237);
    }

    @Test
    void shouldPopulateDiscoveryAddressMapForSingleRange() {
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
    void shouldReturnBrokerAddressForMultipleRanges() {
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
    void shouldThrowForUnknownNodeId() {
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                List.of(new NamedRange("brokers", 0, 1), new NamedRange("controllers", 3, 4))).buildStrategy("cluster");
        String expectedMessage = "Cannot generate node address for node id 5 as it is not contained in the ranges defined for provider with downstream bootstrap "
                + BOOTSTRAP;
        assertThatThrownBy(() -> strategy.getBrokerAddress(5)).isInstanceOf(IllegalArgumentException.class).hasMessage(expectedMessage);
    }

    @Test
    void shouldPopulateDiscoveryAddressMapForMultipleRanges() {
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

    static Stream<Arguments> shouldRejectOverlappingNodeIdRanges() {
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
    void shouldRejectOverlappingNodeIdRanges(List<NamedRange> namedRangeSpecs, String expectedException) {
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                namedRangeSpecs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedException);
    }

    @Test
    void shouldRejectDuplicateRangeNames() {
        List<NamedRange> nodeIdRanges = List.of(new NamedRange("brokers", 0, 1), new NamedRange("brokers", 1, 2));
        assertThatThrownBy(() -> new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                nodeIdRanges))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("non-unique nodeIdRange names discovered: [brokers]");
    }

    @Test
    void shouldReportExclusivePortsForMultipleRanges() {
        List<NamedRange> nodeIdRanges = List.of(new NamedRange("brokers", 0, 1), new NamedRange("controllers", 3, 4));
        NodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(BOOSTRAP_HOSTPORT,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                nodeIdRanges).buildStrategy("cluster");
        assertThat(strategy.getExclusivePorts()).containsExactly(1235, 1236, 1237, 1238, 1239);
    }

    @Test
    void shouldRejectConfigurationThatExceedsMaximumPortNumber() {
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
    void shouldExposeBootstrapAddress() {
        var bootstrapAddress = HostPort.parse(BOOTSTRAP);
        var config = new PortIdentifiesNodeIdentificationStrategy(bootstrapAddress,
                ADVERTISED_BROKER_ADDRESS_PATTERN,
                null,
                null);
        assertThat(config.getBootstrapAddress()).isEqualTo(bootstrapAddress);
    }

    @Test
    void shouldBuildNodeIdentificationStrategy() {
        var bootstrap = HostPort.parse("boot:1234");
        var config = new PortIdentifiesNodeIdentificationStrategy(bootstrap, "mybroker", null, null);
        var strategy = buildNodeIdentificationStrategy(config);
        assertThat(strategy.getClusterBootstrapAddress()).isEqualTo(bootstrap);
    }

    @Test
    void shouldUseDefaultNodeIdRangeWhenNotConfigured() {
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
    void shouldUseConfiguredSingleNodeIdRange() {
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
    void shouldUseConfiguredNodeStartPort() {
        var bootstrap = HostPort.parse("boot:1234");
        var config = new PortIdentifiesNodeIdentificationStrategy(bootstrap, "mybroker", 1240, null);
        var strategy = buildNodeIdentificationStrategy(config);
        assertThat(strategy.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("mybroker:1240"));
    }

    @Test
    void shouldOnlyReportBootstrapPortPriorToBinding() {
        // Given
        var strategy = new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 0),
                ADVERTISED_BROKER_ADDRESS_PATTERN, null, null).buildStrategy("cluster");
        // When/Then - node ports are not known until bootstrap resolves; only port 0 reported
        assertThat(strategy.getExclusivePorts()).containsExactly(0);
    }

    @Test
    void shouldReturnEmptyDiscoveryMapPriorToBootstrapBinding() {
        // Given
        var strategy = new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 0),
                ADVERTISED_BROKER_ADDRESS_PATTERN, null, null).buildStrategy("cluster");
        // When/Then - cannot compute node ports until the resolved bootstrap port is known
        assertThat(strategy.discoveryAddressMap()).isEmpty();
    }

    @Test
    void shouldAdvertiseBrokerPortsRelativeToResolvedBootstrapPort() {
        // Given
        var strategy = new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 0),
                ADVERTISED_BROKER_ADDRESS_PATTERN, null, null).buildStrategy("cluster");
        // When
        strategy.notifyBootstrapPortResolved(54321);
        // Then - node ports are bootstrap + offset (same relationship as for fixed ports)
        assertThat(strategy.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 54322));
        assertThat(strategy.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 54323));
        assertThat(strategy.getBrokerAddress(2)).isEqualTo(new HostPort("broker2.kafka.example.com", 54324));
    }

    @Test
    void shouldPopulateDiscoveryMapRelativeToResolvedBootstrapPort() {
        // Given
        var strategy = new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 0),
                ADVERTISED_BROKER_ADDRESS_PATTERN, null, null).buildStrategy("cluster");
        // When
        strategy.notifyBootstrapPortResolved(54321);
        // Then
        assertThat(strategy.discoveryAddressMap()).isEqualTo(Map.of(
                0, new HostPort("broker0.kafka.example.com", 54322),
                1, new HostPort("broker1.kafka.example.com", 54323),
                2, new HostPort("broker2.kafka.example.com", 54324)));
    }

    @Test
    void shouldTreatExplicitNodeStartPortAsAbsoluteNotRelativeToBootstrap() {
        // Given - explicitly configured nodeStartPort should be an absolute port, not an offset
        var strategy = new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 0),
                ADVERTISED_BROKER_ADDRESS_PATTERN, 5000, null).buildStrategy("cluster");
        // When
        strategy.notifyBootstrapPortResolved(54321);
        // Then - node ports are the explicit absolute values, not relative to resolved bootstrap
        assertThat(strategy.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 5000));
        assertThat(strategy.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 5001));
        assertThat(strategy.getExclusivePorts()).contains(0, 5000, 5001, 5002);
    }

    private NodeIdentificationStrategy buildNodeIdentificationStrategy(PortIdentifiesNodeIdentificationStrategy config) {
        return config.buildStrategy("cluster");
    }

}
