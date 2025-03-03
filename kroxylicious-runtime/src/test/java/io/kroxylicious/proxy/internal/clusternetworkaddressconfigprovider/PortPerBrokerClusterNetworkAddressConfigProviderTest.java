/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig;
import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("removal")
class PortPerBrokerClusterNetworkAddressConfigProviderTest {

    @ParameterizedTest
    @ValueSource(ints = { -1, 0 })
    void badBrokerStartPort(int brokerStartPort) {
        var bootstrap = parse("localhost:1235");
        assertThatThrownBy(() -> new PortPerBrokerClusterNetworkAddressConfigProviderConfig(bootstrap,
                "localhost", brokerStartPort, 0, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("brokerStartPort cannot be less than 1");
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 0 })
    void badNumberOfBrokerPorts(int numberOfBrokerPorts) {
        var bootstrap = parse("localhost:1235");
        assertThatThrownBy(() -> new PortPerBrokerClusterNetworkAddressConfigProviderConfig(bootstrap,
                "localhost", null, 0, numberOfBrokerPorts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("numberOfBrokerPorts cannot be less than 1");
    }

    @ParameterizedTest
    @CsvSource({ "1235,1", "1234,3" })
    void bootstrapBrokerAddressCollision(int brokerStartPort, int numberOfBrokerPorts) {
        var bootstrap = parse("localhost:1235");
        assertThatThrownBy(() -> new PortPerBrokerClusterNetworkAddressConfigProviderConfig(bootstrap,
                "localhost", brokerStartPort, 0, numberOfBrokerPorts))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "portnotallowedinbrokerpattern:1234", "unrecognizedpattern$(foo)", "badpatterncapitalisedbadtoo$(NODEID)" })
    @EmptySource
    void invalidBrokerAddressPatterns(String input) {
        var bootstrap = parse("good:1235");
        assertThatThrownBy(() -> new PortPerBrokerClusterNetworkAddressConfigProviderConfig(bootstrap, input, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "localhost", "127.0.0.1", "[2001:db8::1]", "kafka.example.com", "mybroker$(nodeId)", "mybroker$(nodeId).example.com",
            "twice$(nodeId)allowed$(nodeId)too" })
    void validBrokerAddressPatterns(String input) {
        var bootstrap = parse("good:1235");
        var config = new PortPerBrokerClusterNetworkAddressConfigProviderConfig(bootstrap, input, 1, 0, 5);
        assertThat(config.getBrokerAddressPattern()).isEqualTo(input);

    }

    @Test
    void getBootstrapAddress() {
        var bootstrap = parse("good:1235");
        var config = new PortPerBrokerClusterNetworkAddressConfigProviderConfig(bootstrap, "localhost", 1, 0, 5);
        assertThat(config.getBootstrapAddress()).isEqualTo(bootstrap);
    }

    @Test
    void getBrokerStartPort() {
        var bootstrap = parse("good:1235");
        var config = new PortPerBrokerClusterNetworkAddressConfigProviderConfig(bootstrap, "localhost", 2345, 0, 5);
        assertThat(config.getBrokerStartPort()).isEqualTo(2345);
    }

    @Test
    void portsExhausted() {
        var provider = new PortPerBrokerClusterNetworkAddressConfigProvider().build(
                new PortPerBrokerClusterNetworkAddressConfigProviderConfig(parse("localhost:1235"),
                        "localhost", 1236, 0, 1));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("localhost:1236"));
        assertThatThrownBy(() -> provider.getBrokerAddress(1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void defaultsBrokerPatternBasedOnBootstrapHost() {
        var provider = new PortPerBrokerClusterNetworkAddressConfigProvider().build(
                new PortPerBrokerClusterNetworkAddressConfigProviderConfig(parse("mycluster:1235"), null, 1236, 0, 1237));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("mycluster:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("mycluster:1236"));
    }

    @Test
    void defaultsBrokerStartPortBasedOnBootstrapPort() {
        var provider = new PortPerBrokerClusterNetworkAddressConfigProvider().build(
                new PortPerBrokerClusterNetworkAddressConfigProviderConfig(parse("mycluster:1235"), null, null, 0, 1237));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("mycluster:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("mycluster:1236"));
    }

    @Test
    void defaultsNumberOfBrokerPorts() {
        var provider = new PortPerBrokerClusterNetworkAddressConfigProvider().build(
                new PortPerBrokerClusterNetworkAddressConfigProviderConfig(parse("mycluster:1235"), null, null, 0, null));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("mycluster:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("mycluster:1236"));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(parse("mycluster:1237"));
        assertThat(provider.getBrokerAddress(2)).isEqualTo(parse("mycluster:1238"));
        Map<Integer, HostPort> expectedDiscovery = Map.of(0, parse("mycluster:1236"), 1, parse("mycluster:1237"), 2, parse("mycluster:1238"));
        assertThat(provider.discoveryAddressMap()).containsExactlyInAnyOrderEntriesOf(expectedDiscovery);
        assertThatThrownBy(() -> provider.getBrokerAddress(3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lowestTargetBrokerIdConfigurable() {
        var provider = new PortPerBrokerClusterNetworkAddressConfigProvider().build(
                new PortPerBrokerClusterNetworkAddressConfigProviderConfig(parse("mycluster:1235"), null, null, 2, null));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("mycluster:1235"));
        assertThat(provider.getBrokerAddress(2)).isEqualTo(parse("mycluster:1236"));
        assertThat(provider.getBrokerAddress(3)).isEqualTo(parse("mycluster:1237"));
        assertThat(provider.getBrokerAddress(4)).isEqualTo(parse("mycluster:1238"));
        Map<Integer, HostPort> expectedDiscovery = Map.of(2, parse("mycluster:1236"), 3, parse("mycluster:1237"), 4, parse("mycluster:1238"));
        assertThat(provider.discoveryAddressMap()).containsExactlyInAnyOrderEntriesOf(expectedDiscovery);
        assertThatThrownBy(() -> provider.getBrokerAddress(5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> provider.getBrokerAddress(1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void definesExclusiveAndSharedCorrectly() {
        var provider = new PortPerBrokerClusterNetworkAddressConfigProvider().build(
                new PortPerBrokerClusterNetworkAddressConfigProviderConfig(parse("localhost:1235"),
                        "localhost", 1236, 0, 2));
        assertThat(provider.getExclusivePorts()).containsExactlyInAnyOrder(1235, 1236, 1237);
        assertThat(provider.getSharedPorts()).isEmpty();
    }

    @Test
    void generatesBrokerAddresses() {
        var provider = new PortPerBrokerClusterNetworkAddressConfigProvider().build(
                new PortPerBrokerClusterNetworkAddressConfigProviderConfig(parse("localhost:1235"),
                        "localhost", 1236, 0, 3));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("localhost:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("localhost:1236"));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(parse("localhost:1237"));
    }

    @Test
    void fullyQualifiedHostNames() {
        var provider = new PortPerBrokerClusterNetworkAddressConfigProvider().build(
                new PortPerBrokerClusterNetworkAddressConfigProviderConfig(parse("cluster.kafka.example.com:1235"),
                        "broker.kafka.example.com", 1236,
                        0, 1238));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("cluster.kafka.example.com:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("broker.kafka.example.com:1236"));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(parse("broker.kafka.example.com:1237"));
    }

    @Test
    void fullyQualifiedHostNamesWithNodeInterpolation() {
        var provider = new PortPerBrokerClusterNetworkAddressConfigProvider().build(
                new PortPerBrokerClusterNetworkAddressConfigProviderConfig(parse("cluster.kafka.example.com:1235"),
                        "broker$(nodeId).kafka.example.com",
                        1236, 0, 1238));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("cluster.kafka.example.com:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("broker0.kafka.example.com:1236"));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(parse("broker1.kafka.example.com:1237"));
    }
}