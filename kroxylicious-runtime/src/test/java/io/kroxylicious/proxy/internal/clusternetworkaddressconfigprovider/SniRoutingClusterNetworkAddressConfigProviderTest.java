/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.proxy.HostPortConverter;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig;
import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

@SuppressWarnings("removal")
class SniRoutingClusterNetworkAddressConfigProviderTest {

    private static final HostPort GOOD_HOST_PORT = parse("boot.kafka:1234");

    @Test
    void valid() {
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker$(nodeId)-good");
        assertThat(config).isNotNull();
    }

    @Test
    void missingBootstrap() {
        assertThatThrownBy(() -> new SniRoutingClusterNetworkAddressConfigProviderConfig(null, "broker$(nodeId)-good"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getBootstrapAddressFromConfig() {
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker$(nodeId)-good");
        assertThat(config.getBootstrapAddress()).isEqualTo(GOOD_HOST_PORT);
    }

    @Test
    void mustSupplyAnAdvertisedBrokerAddressPattern() {
        HostPort bootstrapAddress = parse("arbitrary:1235");
        assertThatThrownBy(() -> new SniRoutingClusterNetworkAddressConfigProviderConfig(bootstrapAddress, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("advertisedBrokerAddressPattern cannot be null");
    }

    @ParameterizedTest
    @ValueSource(strings = { "nonodetoken", "recursive$(nodeId$(nodeId))", "capitalisedrejected$(NODEID)", "noportalloweed-in-pattern-part$(nodeId):1234:1234" })
    @NullAndEmptySource
    void invalidAdvertisedBrokerAddressPattern(String input) {
        var goodHostPort = parse("good:1235");
        assertThatThrownBy(() -> new SniRoutingClusterNetworkAddressConfigProviderConfig(goodHostPort, input))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "broker$(nodeId)", "twice$(nodeId)allowed$(nodeId)too", "broker$(nodeId).kafka.com" })
    void validAdvertisedBrokerAddressPattern(String input) {
        var goodHostPort = parse("good:1235");
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(goodHostPort, input);
        assertThat(config).isNotNull();
    }

    @Test
    void getBrokerAddress() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getAdvertisedBrokerAddress() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka"));
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getAdvertisedBrokerAddressPattern() {
        SniRoutingClusterNetworkAddressConfigProviderConfig config = new SniRoutingClusterNetworkAddressConfigProviderConfig(
                GOOD_HOST_PORT, "broker-$(nodeId).kafka");
        assertThat(config.getAdvertisedBrokerAddressPattern()).isEqualTo("broker-$(nodeId).kafka");
    }

    @Test
    void getClusterBootstrapAddress() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka"));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(GOOD_HOST_PORT);
    }

    @Test
    void getSharedPorts() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka"));
        assertThat(provider.getSharedPorts()).isEqualTo(Set.of(GOOD_HOST_PORT.port()));
    }

    @Test
    void requiresServerNameIndication() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka"));
        assertThat(provider.requiresServerNameIndication()).isTrue();
    }

    @Test
    void getBrokerAddressPrefersAdvertisedPortIfProvided() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka:443"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getAdvertisedPortPrefersAdvertisedBrokerAddressIfSpecified() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka:443"));
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:443"));
    }

    static Stream<Arguments> getBrokerIdFromBrokerAddress() {
        return Stream.of(
                argumentSet("broker 0", "broker-0.kafka:1234", 0),
                argumentSet("broker 99", "broker-99.kafka:1234", 99),
                argumentSet("RFC 4343 case insensitive", "BROKER-0.KAFKA:1234", 0),
                argumentSet("port mismatch", "broker-0.kafka:1235", null),
                argumentSet("host mismatch", "broker-0.another:1234", null),
                argumentSet("RE anchoring", "0.kafka:1234", null),
                argumentSet("RE anchoring", "start.broker-0.kafka.end:1234", null),
                argumentSet("RE metacharacters in advertisedBrokerAddressPattern escaped", "broker-0xkafka:1234", null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void getBrokerIdFromBrokerAddress(@ConvertWith(HostPortConverter.class) HostPort address, Integer expected) {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka"));

        assertThat(provider.getBrokerIdFromBrokerAddress(address)).isEqualTo(expected);
    }

    @Test
    void badNodeId() {
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka");
        var service = new SniRoutingClusterNetworkAddressConfigProvider();
        var provider = service.build(config);

        assertThatThrownBy(() -> provider.getBrokerAddress(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
