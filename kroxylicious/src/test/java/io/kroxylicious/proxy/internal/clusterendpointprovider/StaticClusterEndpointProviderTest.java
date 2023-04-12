/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.clusterendpointprovider.StaticClusterEndpointProvider.StaticClusterEndpointProviderConfig;

import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StaticClusterEndpointProviderTest {

    @Test
    void noBrokerAddresses() {
        assertThrows(IllegalArgumentException.class, () -> new StaticClusterEndpointProviderConfig(parse("good:1235"), Map.of()));
    }

    @Test
    void brokerAddressDuplicated() {
        assertThrows(IllegalArgumentException.class,
                () -> new StaticClusterEndpointProviderConfig(parse("good:1235"), Map.of(0, parse("duplicate:1234"), 1, parse("duplicate:1234"))));
    }

    @Test
    void validArguments() {
        var provider = new StaticClusterEndpointProvider(
                new StaticClusterEndpointProviderConfig(parse("boot:1235"), Map.of(0, parse("good:1234"), 1, parse("good:2345"))));
        assertEquals(parse("boot:1235"), provider.getClusterBootstrapAddress());
        assertEquals(parse("good:1234"), provider.getBrokerAddress(0));
        assertEquals(parse("good:2345"), provider.getBrokerAddress(1));
        assertEquals(2, provider.getNumberOfBrokerEndpointsToPrebind());
    }

    @Test
    void defaultsBrokerZero() {
        var provider = new StaticClusterEndpointProvider(new StaticClusterEndpointProviderConfig(parse("boot:1235"), null));
        assertEquals(parse("boot:1235"), provider.getClusterBootstrapAddress());
        assertEquals(parse("boot:1235"), provider.getBrokerAddress(0));
        assertEquals(1, provider.getNumberOfBrokerEndpointsToPrebind());
    }
}
