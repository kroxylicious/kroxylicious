/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusterendpointprovider;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.clusterendpointprovider.StaticClusterEndpointProvider.StaticClusterEndpointProviderConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StaticClusterEndpointProviderTest {

    @Test
    void bootstrapPoorlyFormed() {
        assertThrows(IllegalArgumentException.class, () -> new StaticClusterEndpointProviderConfig("bad", Map.of()));
    }

    @Test
    void noBrokerAddresses() {
        assertThrows(IllegalArgumentException.class, () -> new StaticClusterEndpointProviderConfig("good:1235", Map.of()));
    }

    @Test
    void brokerAddressPoorlyFormed() {
        assertThrows(IllegalArgumentException.class, () -> new StaticClusterEndpointProviderConfig("good:1235", Map.of(0, "bad")));
    }

    @Test
    void brokerAddressNodeIdRangeError() {
        assertThrows(IllegalArgumentException.class, () -> new StaticClusterEndpointProviderConfig("good:1235", Map.of(0, "good:1234", 2, "good:2345")));
    }

    @Test
    void brokerAddressDuplicated() {
        assertThrows(IllegalArgumentException.class, () -> new StaticClusterEndpointProviderConfig("good:1235", Map.of(0, "duplicate:1234", 1, "duplicate:1234")));
    }

    @Test
    void validArguments() {
        var provider = new StaticClusterEndpointProvider(new StaticClusterEndpointProviderConfig("boot:1235", Map.of(0, "good:1234", 1, "good:2345")));
        assertEquals("boot:1235", provider.getClusterBootstrapAddress());
        assertEquals("good:1234", provider.getBrokerAddress(0));
        assertEquals("good:2345", provider.getBrokerAddress(1));
        assertEquals(2, provider.getNumberOfBrokerEndpointsToPrebind());
    }

    @Test
    void defaultsBrokerZero() {
        var provider = new StaticClusterEndpointProvider(new StaticClusterEndpointProviderConfig("boot:1235", null));
        assertEquals("boot:1235", provider.getClusterBootstrapAddress());
        assertEquals("boot:1235", provider.getBrokerAddress(0));
        assertEquals(1, provider.getNumberOfBrokerEndpointsToPrebind());
    }
}