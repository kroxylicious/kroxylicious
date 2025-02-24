/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.stream.Stream;

import org.assertj.core.api.ThrowableAssert;
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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class SniRoutingClusterNetworkAddressConfigProviderTest {

    private static final HostPort GOOD_HOST_PORT = parse("boot.kafka:1234");

    @Test
    void valid() {
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker$(nodeId)-good", null);
        assertThat(config).isNotNull();
    }

    static Stream<Arguments> mustSupplyABrokerAddressPatternOrAdvertisedBrokerAddressPattern() {
        return Stream.of(argumentSet("with brokerAddressPattern", "broker-$(nodeId)-good", null, true),
                argumentSet("with advertisedBrokerAddressPattern", null, "broker-$(nodeId)-good", true),
                argumentSet("both disallowed", "broker-$(nodeId)-good", "broker-$(nodeId)-good", false),
                argumentSet("neither disallowed", null, null, false));
    }

    @ParameterizedTest
    @MethodSource
    void mustSupplyABrokerAddressPatternOrAdvertisedBrokerAddressPattern(String brokerAddress, String advertisedBrokerAddress, boolean valid) {
        ThrowableAssert.ThrowingCallable test = () -> {
            new SniRoutingClusterNetworkAddressConfigProviderConfig(parse("arbitrary:1235"), brokerAddress, advertisedBrokerAddress);
        };
        if (valid) {
            assertThatCode(test).doesNotThrowAnyException();
        }
        else {
            assertThatThrownBy(test).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "nonodetoken", "recursive$(nodeId$(nodeId))", "capitalisedrejected$(NODEID)", "noportalloweed-in-pattern-part$(nodeId):1234:1234" })
    @NullAndEmptySource
    void invalidBrokerAddressPattern(String input) {
        var goodHostPort = parse("good:1235");
        assertThatThrownBy(() -> new SniRoutingClusterNetworkAddressConfigProviderConfig(goodHostPort, input, (String) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "nonodetoken", "recursive$(nodeId$(nodeId))", "capitalisedrejected$(NODEID)", "noportalloweed-in-pattern-part$(nodeId):1234:1234" })
    @NullAndEmptySource
    void invalidAdvertisedBrokerAddressPattern(String input) {
        var goodHostPort = parse("good:1235");
        assertThatThrownBy(() -> new SniRoutingClusterNetworkAddressConfigProviderConfig(goodHostPort, (String) null, input))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "broker$(nodeId)", "twice$(nodeId)allowed$(nodeId)too", "broker$(nodeId).kafka.com" })
    void validBrokerAddressPatterns(String input) {
        var goodHostPort = parse("good:1235");
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(goodHostPort, input, (String) null);
        assertThat(config).isNotNull();
    }

    @Test
    void getBrokerAddress() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka", (String) null));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getAdvertisedPortForDeprecatedBrokerAddressPatternIsBootstrapBrokerAddress() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka", (String) null));
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getBrokerAddressPrefersAdvertisedPortIfProvided() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, (String) null, "broker-$(nodeId).kafka:443"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getAdvertisedPortPrefersAdvertisedBrokerAddressIfSpecified() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, null, "broker-$(nodeId).kafka:443"));
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:443"));
    }

    @Test
    void getAdvertisedPortDefaultsToBootstrapBrokerAddressIfNotSpecified() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, (String) null, "broker-$(nodeId).kafka"));
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
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
                argumentSet("RE metacharacters in brokerAddressPattern escaped", "broker-0xkafka:1234", null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void getBrokerIdFromBrokerAddress(@ConvertWith(HostPortConverter.class) HostPort address, Integer expected) {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka", (String) null));

        assertThat(provider.getBrokerIdFromBrokerAddress(address)).isEqualTo(expected);
    }

    @Test
    void badNodeId() {
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka", (String) null);
        var provider = new SniRoutingClusterNetworkAddressConfigProvider();

        assertThatThrownBy(() -> provider.build(config)
                .getBrokerAddress(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
