/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.proxy.internal.clusterendpointprovider.StaticClusterEndpointProvider.StaticClusterEndpointProviderConfig;
import io.kroxylicious.proxy.service.ClusterEndpointProvider.EndpointMatchResult;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StaticClusterEndpointProviderTest {

    @Test
    void noBrokerAddresses() {
        assertThrows(IllegalArgumentException.class, () -> new StaticClusterEndpointProviderConfig(parse("good:1235"), Map.of()));
    }

    @Test
    void rejectsDuplicatedBrokerAddresses() {
        assertThrows(IllegalArgumentException.class,
                () -> new StaticClusterEndpointProviderConfig(parse("good:1235"), Map.of(0, parse("duplicate:1234"), 1, parse("duplicate:1234"))));
    }

    @Test
    void rejectsDuplicatedBrokerPorts() {
        assertThrows(IllegalArgumentException.class,
                () -> new StaticClusterEndpointProviderConfig(parse("foo:1235"), Map.of(0, parse("bar:1234"), 1, parse("baz:1234"))));
    }

    @Test
    void validArguments() {
        var provider = new StaticClusterEndpointProvider(
                new StaticClusterEndpointProviderConfig(parse("boot:1235"), Map.of(0, parse("good:1234"), 1, parse("good:2345"))));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("boot:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("good:1234"));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(parse("good:2345"));
        assertThat(provider.getNumberOfBrokerEndpointsToPrebind()).isEqualTo(2);
    }

    @Test
    void defaultsBrokerZero() {
        var provider = new StaticClusterEndpointProvider(new StaticClusterEndpointProviderConfig(parse("boot:1235"), null));
        HostPort parse1 = parse("boot:1235");
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse1);
        HostPort parse = parse("boot:1235");
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse);
        assertThat(provider.getNumberOfBrokerEndpointsToPrebind()).isEqualTo(1);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "idontcare" })
    void findsBootstrapMatchByPort(String sniHostname) {
        var provider = new StaticClusterEndpointProvider(new StaticClusterEndpointProviderConfig(parse("boot:1234"), null));
        assertThat(provider.hasMatchingEndpoint(sniHostname, 1234)).isEqualTo(new EndpointMatchResult(true, null));
        assertThat(provider.hasMatchingEndpoint(sniHostname, 1235)).isEqualTo(new EndpointMatchResult(false, null));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "idontcare" })
    void findsBrokerMatchByPort(String sniHostname) {
        var provider = new StaticClusterEndpointProvider(
                new StaticClusterEndpointProviderConfig(parse("boot:1234"), Map.of(0, parse("broker:1235"), 1, parse("broker:1236"))));
        assertThat(provider.hasMatchingEndpoint(sniHostname, 1235)).isEqualTo(new EndpointMatchResult(true, 0));
        assertThat(provider.hasMatchingEndpoint(sniHostname, 1236)).isEqualTo(new EndpointMatchResult(true, 1));
        assertThat(provider.hasMatchingEndpoint(sniHostname, 1237)).isEqualTo(new EndpointMatchResult(false, null));
    }
}
