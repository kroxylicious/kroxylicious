/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

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
import static org.junit.jupiter.api.Assertions.assertThrows;

class SniRoutingClusterNetworkAddressConfigProviderTest {

    @Test
    void valid() {
        new SniRoutingClusterNetworkAddressConfigProviderConfig(
                parse("good:1235"),
                "broker$(nodeId)-good"
        );

    }

    @ParameterizedTest
    @ValueSource(strings = { "nonodetoken", "recursive$(nodeId$(nodeId))", "capitalisedrejected$(NODEID)", "noportalloweed$(nodeId):1234" })
    @NullAndEmptySource
    void invalidBrokerAddressPattern(String input) {
        assertThrows(
                IllegalArgumentException.class,
                () -> new SniRoutingClusterNetworkAddressConfigProviderConfig(parse("good:1235"), input)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = { "broker$(nodeId)", "twice$(nodeId)allowed$(nodeId)too", "broker$(nodeId).kafka.com" })
    void validBrokerAddressPatterns(String input) {
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(parse("good:1235"), input);
        assertThat(config).isNotNull();
    }

    @Test
    void getBrokerAddress() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(
                        parse("boot.kafka:1234"),
                        "broker-$(nodeId).kafka"
                )
        );
        assertThat(provider.getBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    public static Stream<Arguments> getBrokerIdFromBrokerAddress() {
        return Stream.of(
                Arguments.of("broker 0", "broker-0.kafka:1234", 0),
                Arguments.of("broker 99", "broker-99.kafka:1234", 99),
                Arguments.of("RFC 4343 case insensitive", "BROKER-0.KAFKA:1234", 0),
                Arguments.of("port mismatch", "broker-0.kafka:1235", null),
                Arguments.of("host mismatch", "broker-0.another:1234", null),
                Arguments.of("RE anchoring", "0.kafka:1234", null),
                Arguments.of("RE anchoring", "start.broker-0.kafka.end:1234", null),
                Arguments.of("RE metacharacters in brokerAddressPattern escaped", "broker-0xkafka:1234", null)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void getBrokerIdFromBrokerAddress(String name, @ConvertWith(HostPortConverter.class)
    HostPort address, Integer expected) {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(
                        parse("boot.kafka:1234"),
                        "broker-$(nodeId).kafka"
                )
        );

        assertThat(provider.getBrokerIdFromBrokerAddress(address)).isEqualTo(expected);
    }

    @Test
    void badNodeId() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new SniRoutingClusterNetworkAddressConfigProvider(
                        new SniRoutingClusterNetworkAddressConfigProviderConfig(parse("boot.kafka:1234"), "broker-$(nodeId).kafka")
                )
                 .getBrokerAddress(-1)
        );
    }
}
