/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.clusterendpointprovider.StaticClusterEndpointConfigProvider.StaticClusterEndpointProviderConfig;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StaticClusterEndpointConfigProviderTest {

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
        var provider = new StaticClusterEndpointConfigProvider(
                new StaticClusterEndpointProviderConfig(parse("boot:1235"), Map.of(0, parse("good:1234"), 1, parse("good:2345"))));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse("boot:1235"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse("good:1234"));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(parse("good:2345"));
        assertThat(provider.getNumberOfBrokerEndpointsToPrebind()).isEqualTo(2);
    }

    @Test
    void defaultsBrokerZero() {
        var provider = new StaticClusterEndpointConfigProvider(new StaticClusterEndpointProviderConfig(parse("boot:1235"), null));
        HostPort parse1 = parse("boot:1235");
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(parse1);
        HostPort parse = parse("boot:1235");
        assertThat(provider.getBrokerAddress(0)).isEqualTo(parse);
        assertThat(provider.getNumberOfBrokerEndpointsToPrebind()).isEqualTo(1);
    }
}
