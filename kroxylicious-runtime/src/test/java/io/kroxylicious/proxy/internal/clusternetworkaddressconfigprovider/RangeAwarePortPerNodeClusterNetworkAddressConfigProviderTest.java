/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.Range;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RangeAwarePortPerNodeClusterNetworkAddressConfigProviderTest {

    public static final String BOOTSTRAP_HOST = "cluster.kafka.example.com";
    private static final String BOOTSTRAP = BOOTSTRAP_HOST + ":1235";

    @Test
    void rangesMustBeNonEmpty() {
        assertThatThrownBy(() -> new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of()))).isInstanceOf(IllegalArgumentException.class).hasMessage("node id ranges empty");
    }

    @Test
    void brokerAddressSingleRange() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new Range("brokers", "[0,1]"))));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 1236));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 1237));
    }

    @Test
    void brokerAddressPortsInferredFromBootstrapIfNotExplicitlySupplied() {
        List<Range> ranges = List.of(new Range("brokers", "[0,1]"));
        HostPort bootstrapAddress = HostPort.parse(BOOTSTRAP);
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                        bootstrapAddress, "broker$(nodeId).kafka.example.com",
                        null, ranges));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 1236));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 1237));
    }

    @Test
    void brokerAddressInferredFromBootstrapIfNotExplicitlySupplied() {
        List<Range> ranges = List.of(new Range("brokers", "[0,1]"));
        HostPort bootstrapAddress = HostPort.parse(BOOTSTRAP);
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                        bootstrapAddress, null,
                        null, ranges));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(new HostPort(BOOTSTRAP_HOST, 1236));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(new HostPort(BOOTSTRAP_HOST, 1237));
    }

    @Test
    void nodeAddressPatternCannotBeBlank() {
        assertThatThrownBy(() -> new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                HostPort.parse(BOOTSTRAP), "",
                null, List.of(new Range("brokers", "[0,1]"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeAddressPattern cannot be blank");
    }

    @Test
    void nodeAddressPatternCannotContainUnexpectedReplacementPatterns() {
        assertThatThrownBy(() -> new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                HostPort.parse(BOOTSTRAP), "node-$(typoedNodeId).broker.com",
                null, List.of(new Range("brokers", "[0,1]"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeAddressPattern contains an unexpected replacement token '$(typoedNodeId)'");
    }

    @Test
    void nodeAddressPatternCannotContainPort() {
        assertThatThrownBy(() -> new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                HostPort.parse(BOOTSTRAP), "localhost:8080",
                null, List.of(new Range("brokers", "[0,1]"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeAddressPattern cannot have port specifier.  Found port : 8080 within localhost:8080");
    }

    @Test
    void nodeStartPortCannotBeLessThanOne() {
        assertThatThrownBy(() -> new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                HostPort.parse(BOOTSTRAP), "localhost",
                0, List.of(new Range("brokers", "[0,1]"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nodeStartPort cannot be less than 1");
    }

    @Test
    void nodePortRangeCannotCollideWithBootstrapPort() {
        assertThatThrownBy(() -> new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                HostPort.parse(BOOTSTRAP_HOST + ":1235"), "localhost",
                // node id 1 will be assigned port 1235 and collide with bootstrap
                1234, List.of(new Range("brokers", "[0,2]"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("the port used by the bootstrap address (1235) collides with the node port range");
    }

    @Test
    void getClusterBootstrap() {
        List<Range> ranges = List.of(new Range("brokers", "[0,1]"));
        HostPort bootstrapAddress = HostPort.parse(BOOTSTRAP);
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                        bootstrapAddress, "broker$(nodeId).kafka.example.com",
                        1236, ranges));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(bootstrapAddress);
    }

    @Test
    void exclusivePortsSingleRange() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new Range("brokers", "[0,1]"))));
        assertThat(provider.getExclusivePorts()).containsExactly(1235, 1236, 1237);
    }

    @Test
    void discoveryAddressMapSingleRange() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new Range("brokers", "[0,1]"))));

        Map<Integer, HostPort> expected = Map.of(
                0, new HostPort("broker0.kafka.example.com", 1236),
                1, new HostPort("broker1.kafka.example.com", 1237));
        assertThat(provider.discoveryAddressMap()).isEqualTo(expected);
    }

    @Test
    void brokerAddressMultipleRanges() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new Range("brokers", "[0,1]"), new Range("controllers", "[3,4]"))));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(new HostPort("broker0.kafka.example.com", 1236));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(new HostPort("broker1.kafka.example.com", 1237));
        assertThat(provider.getBrokerAddress(3)).isEqualTo(new HostPort("broker3.kafka.example.com", 1238));
        assertThat(provider.getBrokerAddress(4)).isEqualTo(new HostPort("broker4.kafka.example.com", 1239));
    }

    @Test
    void brokerAddressUnknownNodeId() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new Range("brokers", "[0,1]"), new Range("controllers", "[3,4]"))));
        String expectedMessage = "Cannot generate node address for node id 5 as it is not contained in the ranges defined for provider with downstream bootstrap "
                + BOOTSTRAP;
        assertThatThrownBy(() -> provider.getBrokerAddress(5)).isInstanceOf(IllegalArgumentException.class).hasMessage(expectedMessage);
    }

    @Test
    void discoveryAddressMapMultipleRanges() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new Range("brokers", "[0,1]"), new Range("controllers", "[3,4]"))));

        Map<Integer, HostPort> expected = Map.of(
                0, new HostPort("broker0.kafka.example.com", 1236),
                1, new HostPort("broker1.kafka.example.com", 1237),
                3, new HostPort("broker3.kafka.example.com", 1238),
                4, new HostPort("broker4.kafka.example.com", 1239));
        assertThat(provider.discoveryAddressMap()).isEqualTo(expected);
    }

    @Test
    void exclusivePortsMultipleRanges() {
        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider provider = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider(
                getConfig(List.of(new Range("brokers", "[0,1]"), new Range("controllers", "[3,4]"))));
        assertThat(provider.getExclusivePorts()).containsExactly(1235, 1236, 1237, 1238, 1239);
    }

    private static @NonNull RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig getConfig(List<Range> ranges) {
        HostPort bootstrapAddress = HostPort.parse(BOOTSTRAP);
        return new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                bootstrapAddress, "broker$(nodeId).kafka.example.com",
                1236, ranges);
    }

}
