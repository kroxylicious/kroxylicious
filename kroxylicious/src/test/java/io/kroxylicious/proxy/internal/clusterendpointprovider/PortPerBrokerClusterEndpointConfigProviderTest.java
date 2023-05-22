/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.proxy.internal.clusterendpointprovider.PortPerBrokerClusterEndpointConfigProvider.PortPerBrokerClusterEndpointProviderConfig;

import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PortPerBrokerClusterEndpointConfigProviderTest {

    @ParameterizedTest
    @CsvSource({ "0,1", "-1,1", "4,-1", "10,0" })
    void badBrokerPortDefinition(int brokerStartPort, int numberOfBrokerPorts) {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new PortPerBrokerClusterEndpointProviderConfig(parse("localhost:1235"), "localhost:$(portNumber)", brokerStartPort, numberOfBrokerPorts);
        });
    }

    @ParameterizedTest
    @CsvSource({ "1235,1", "1234,3" })
    void bootstrapBrokerAddressCollision(int brokerStartPort, int numberOfBrokerPorts) {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new PortPerBrokerClusterEndpointProviderConfig(parse("localhost:1235"), "localhost:$(portNumber)", brokerStartPort, numberOfBrokerPorts);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = { "notgood:$(foo)", "toomany$(portNumber)$(portNumber)", "toomanyrecursive$(portNumber(portNumber))", "shouty$(PORTNUMBER)badtoo",
            "not.at.$(portNumber).end:1234" })
    @EmptySource
    void invalidBrokerPattern(String input) {
        assertThrows(IllegalArgumentException.class, () -> new PortPerBrokerClusterEndpointProviderConfig(parse("good:1235"), input, 1, 5));
    }

    @Test
    void portsExhausted() {
        var provider = new PortPerBrokerClusterEndpointConfigProvider(
                new PortPerBrokerClusterEndpointProviderConfig(parse("localhost:1235"), "localhost:$(portNumber)", 1236, 1));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("localhost:1236"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            provider.getBrokerAddress(1);
        });
    }

    @Test
    void defaultsBrokerPatternBasedOnBootstrapHost() {
        var provider = new PortPerBrokerClusterEndpointConfigProvider(
                new PortPerBrokerClusterEndpointProviderConfig(parse("mycluster:1235"), null, 1236, 1237));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("mycluster:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("mycluster:1236"));
    }

    @Test
    void defaultsBrokerStartPortBasedOnBootstrapPort() {
        var provider = new PortPerBrokerClusterEndpointConfigProvider(
                new PortPerBrokerClusterEndpointProviderConfig(parse("mycluster:1235"), null, null, 1237));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("mycluster:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("mycluster:1236"));
    }

    @Test
    void defaultsNumberOfBrokerPorts() {
        var provider = new PortPerBrokerClusterEndpointConfigProvider(
                new PortPerBrokerClusterEndpointProviderConfig(parse("mycluster:1235"), null, null, null));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("mycluster:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("mycluster:1236"));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(parse("mycluster:1237"));
        assertThat(provider.getBrokerAddress(2)).isEqualTo(parse("mycluster:1238"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            provider.getBrokerAddress(3);
        });
    }

    @Test
    void definesExclusiveAndSharedCorrectly() {
        var provider = new PortPerBrokerClusterEndpointConfigProvider(
                new PortPerBrokerClusterEndpointProviderConfig(parse("localhost:1235"), "localhost:$(portNumber)", 1236, 2));
        assertThat(provider.getExclusivePorts()).containsExactlyInAnyOrder(1235, 1236, 1237);
        assertThat(provider.getSharedPorts()).isEmpty();
    }

    @Test
    void generatesBrokerAddresses() {
        var provider = new PortPerBrokerClusterEndpointConfigProvider(
                new PortPerBrokerClusterEndpointProviderConfig(parse("localhost:1235"), "localhost:$(portNumber)", 1236, 3));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("localhost:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("localhost:1236"));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(parse("localhost:1237"));
        assertThat(provider.getNumberOfBrokerEndpointsToPrebind()).isEqualTo(1);
    }

    @Test
    void fullyQualifiedHostNames() {
        var provider = new PortPerBrokerClusterEndpointConfigProvider(
                new PortPerBrokerClusterEndpointProviderConfig(parse("cluster.kafka.example.com:1235"), "broker.kafka.example.com:$(portNumber)", 1236, 1238));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("cluster.kafka.example.com:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("broker.kafka.example.com:1236"));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(parse("broker.kafka.example.com:1237"));
        assertThat(provider.getNumberOfBrokerEndpointsToPrebind()).isEqualTo(1);
    }

    @Test
    void fullyQualifiedHostNamesWithNodeInterpolation() {
        var provider = new PortPerBrokerClusterEndpointConfigProvider(
                new PortPerBrokerClusterEndpointProviderConfig(parse("cluster.kafka.example.com:1235"), "broker$(nodeId).kafka.example.com:$(portNumber)", 1236, 1238));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("cluster.kafka.example.com:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("broker0.kafka.example.com:1236"));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(parse("broker1.kafka.example.com:1237"));
        assertThat(provider.getNumberOfBrokerEndpointsToPrebind()).isEqualTo(1);
    }
}