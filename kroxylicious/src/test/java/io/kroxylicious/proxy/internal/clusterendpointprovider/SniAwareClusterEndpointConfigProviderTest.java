/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.proxy.HostPortConverter;
import io.kroxylicious.proxy.internal.clusterendpointprovider.SniAwareClusterEndpointConfigProvider.SniAwareClusterEndpointProviderConfig;
import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SniAwareClusterEndpointConfigProviderTest {

    @Test
    void valid() {
        new SniAwareClusterEndpointProviderConfig(
                parse("good:1235"), "broker$(nodeId)-good");

    }

    @ParameterizedTest
    @ValueSource(strings = { "notgood", "toomany$(nodeId)$(nodeId)", "toomanyrecursive$(nodeId$(nodeId))", "shouty$(NODEID)badtoo" })
    @NullAndEmptySource
    void invalidBrokerPattern(String input) {
        assertThrows(IllegalArgumentException.class, () -> new SniAwareClusterEndpointProviderConfig(parse("good:1235"), input));
    }

    @ParameterizedTest
    @ValueSource(strings = { "boot:1234", "BOOT:1234", "BooT:1234" })
    void findsBootstrapMatch(@ConvertWith(HostPortConverter.class) HostPort data) {
        var provider = new SniAwareClusterEndpointConfigProvider(new SniAwareClusterEndpointProviderConfig(parse("boot:1234"), "unused$(nodeId)"));
        assertThat(provider.hasMatchingEndpoint(data.host(), data.port())).isEqualTo(new ClusterEndpointConfigProvider.EndpointMatchResult(true, null));
    }

    @ParameterizedTest
    @ValueSource(strings = { "boot:1235", "boot.local:1234" })
    void failsToFindBootstrapMatch(@ConvertWith(HostPortConverter.class) HostPort data) {
        var provider = new SniAwareClusterEndpointConfigProvider(new SniAwareClusterEndpointProviderConfig(parse("boot:1234"), "unused$(nodeId)"));
        assertThat(provider.hasMatchingEndpoint(data.host(), data.port())).isEqualTo(new ClusterEndpointConfigProvider.EndpointMatchResult(false, null));
    }

    @ParameterizedTest
    @CsvSource(value = { "broker-0.kafka:1234,0", "broker-1.kafka:1234,1", "broker-10.kafka:1234,10" })
    void findsBrokerAddressMatch(@ConvertWith(HostPortConverter.class) HostPort data, int expectedNodeId) {
        var provider = new SniAwareClusterEndpointConfigProvider(new SniAwareClusterEndpointProviderConfig(parse("boot.kafka:1234"), "broker-$(nodeId).kafka"));
        assertThat(provider.hasMatchingEndpoint(data.host(), data.port())).isEqualTo(new ClusterEndpointConfigProvider.EndpointMatchResult(true, expectedNodeId));
    }

    @ParameterizedTest
    @ValueSource(strings = { "broker--1.kafka:1234", "broker-0.kafka:1235", "broker-10.local:1234" })
    void failsToFindBrokerAddressMatch(@ConvertWith(HostPortConverter.class) HostPort data) {
        var provider = new SniAwareClusterEndpointConfigProvider(new SniAwareClusterEndpointProviderConfig(parse("boot.kafka:1234"), "broker-$(nodeId).kafka"));
        assertThat(provider.hasMatchingEndpoint(data.host(), data.port())).isEqualTo(new ClusterEndpointConfigProvider.EndpointMatchResult(false, null));
    }

    @Test
    void noSni() {
        var provider = new SniAwareClusterEndpointConfigProvider(new SniAwareClusterEndpointProviderConfig(parse("good:1235"), "broker$(nodeId)-good"));
        assertThat(provider.hasMatchingEndpoint(null, 1234)).isEqualTo(new ClusterEndpointConfigProvider.EndpointMatchResult(false, null));
    }

    @Test
    void getBrokerAddress() {
        var provider = new SniAwareClusterEndpointConfigProvider(new SniAwareClusterEndpointProviderConfig(parse("boot.kafka:1234"), "broker-$(nodeId).kafka"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void badNodeId() {
        assertThrows(IllegalArgumentException.class, () -> new SniAwareClusterEndpointConfigProvider(
                new SniAwareClusterEndpointProviderConfig(parse("boot.kafka:1234"), "broker-$(nodeId).kafka")).getBrokerAddress(-1));
    }
}