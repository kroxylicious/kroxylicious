/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.message.RequestHeaderData;

import static org.assertj.core.api.Assertions.assertThat;

class FidelityCheckTest {

    @Test
    void shouldRoundTripPopulatedFieldsThroughKafka() {
        // Given
        RequestHeaderData ours = new RequestHeaderData()
                .setRequestApiKey((short) 18)
                .setRequestApiVersion((short) 7)
                .setCorrelationId(0x01020304)
                .setClientId("kroxylicious-client");

        // When
        FidelityCheck.RoundTrip roundTrip = FidelityCheck.throughKafka(
                ours, new org.apache.kafka.common.message.RequestHeaderData(), (short) 2);

        // Then
        assertThat(roundTrip.roundTripped()).isEqualTo(roundTrip.original());
        assertThat(roundTrip.original()).isNotEmpty();
        assertThat(roundTrip).satisfies(FidelityCheck.RoundTrip::assertAllBytesConsumed);
    }

    @Test
    void shouldRoundTripPopulatedFieldsThroughKroxylicious() {
        // Given
        org.apache.kafka.common.message.RequestHeaderData kafka = new org.apache.kafka.common.message.RequestHeaderData()
                .setRequestApiKey((short) 18)
                .setRequestApiVersion((short) 7)
                .setCorrelationId(0x01020304)
                .setClientId("kroxylicious-client");

        // When
        FidelityCheck.RoundTrip roundTrip = FidelityCheck.throughKroxylicious(kafka, new RequestHeaderData(), (short) 2);

        // Then
        assertThat(roundTrip.roundTripped()).isEqualTo(roundTrip.original());
        assertThat(roundTrip.original()).isNotEmpty();
        assertThat(roundTrip).satisfies(FidelityCheck.RoundTrip::assertAllBytesConsumed);
    }
}