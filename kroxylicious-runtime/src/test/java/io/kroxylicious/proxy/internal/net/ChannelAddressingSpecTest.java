/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChannelAddressingSpecTest {

    @Test
    void identifiesSingleCandidateThatRecognisesConnection() {
        // Given
        var gateway = gatewayReturning(new AddressingSpec.Target.Bootstrap());
        var spec = new ChannelAddressingSpec(List.of(gateway));

        // When
        var result = spec.identify(1234, null);

        // Then
        assertThat(result).contains(new ChannelAddressingSpec.Match(gateway, new AddressingSpec.Target.Bootstrap()));
    }

    @Test
    void returnsEmptyWhenSingleCandidateDoesNotRecogniseConnection() {
        // Given
        var gateway = gatewayReturning(new AddressingSpec.Target.NotRecognised());
        var spec = new ChannelAddressingSpec(List.of(gateway));

        // When
        var result = spec.identify(1234, "unrelated.example.com");

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void identifiesTheCandidateThatRecognisesConnectionAmongOthersThatDoNot() {
        // Given
        var unrelatedGateway = gatewayReturning(new AddressingSpec.Target.NotRecognised());
        var matchingGateway = gatewayReturning(new AddressingSpec.Target.Node(5));
        var spec = new ChannelAddressingSpec(List.of(unrelatedGateway, matchingGateway));

        // When
        var result = spec.identify(1234, "broker-5.example.com");

        // Then
        assertThat(result).contains(new ChannelAddressingSpec.Match(matchingGateway, new AddressingSpec.Target.Node(5)));
    }

    @Test
    void returnsEmptyWhenNoCandidateRecognisesConnection() {
        // Given
        var gatewayA = gatewayReturning(new AddressingSpec.Target.NotRecognised());
        var gatewayB = gatewayReturning(new AddressingSpec.Target.NotRecognised());
        var spec = new ChannelAddressingSpec(List.of(gatewayA, gatewayB));

        // When
        var result = spec.identify(1234, "unrelated.example.com");

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void returnsFirstMatchingCandidateWhenMultipleRecognise() {
        // Given
        var firstGateway = gatewayReturning(new AddressingSpec.Target.Bootstrap());
        var secondGateway = gatewayReturning(new AddressingSpec.Target.Bootstrap());
        var spec = new ChannelAddressingSpec(List.of(firstGateway, secondGateway));

        // When
        var result = spec.identify(1234, null);

        // Then
        assertThat(result).contains(new ChannelAddressingSpec.Match(firstGateway, new AddressingSpec.Target.Bootstrap()));
    }

    private static EndpointGateway gatewayReturning(AddressingSpec.Target target) {
        var gateway = mock(EndpointGateway.class);
        var addressingSpec = mock(AddressingSpec.class);
        when(addressingSpec.identify(anyInt(), anyString())).thenReturn(target);
        when(addressingSpec.identify(anyInt(), isNull())).thenReturn(target);
        when(gateway.addressingSpec()).thenReturn(addressingSpec);
        return gateway;
    }
}
