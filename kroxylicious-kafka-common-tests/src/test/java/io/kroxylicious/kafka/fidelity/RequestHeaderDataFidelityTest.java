/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kafka.fidelity;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kafka.common.message.RequestHeaderData;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@link RequestHeaderData} round-trips byte-for-byte against
 * {@code org.apache.kafka.common.message.RequestHeaderData}, in both directions, at every version it
 * supports. This is the first spec proven by the fidelity harness; {@code AllDataClassesFidelityTest}
 * generalizes this same check across every spec.
 */
class RequestHeaderDataFidelityTest {

    static Stream<Short> supportedVersions() {
        RequestHeaderData reference = new RequestHeaderData();
        return IntStream.rangeClosed(reference.lowestSupportedVersion(), reference.highestSupportedVersion())
                .mapToObj(version -> (short) version);
    }

    @ParameterizedTest
    @MethodSource("supportedVersions")
    void roundTripsThroughKafka(short version) {
        RequestHeaderData ours = new RequestHeaderData();

        FidelityCheck.RoundTrip roundTrip = FidelityCheck.throughKafka(
                ours, new org.apache.kafka.common.message.RequestHeaderData(), version);

        assertThat(roundTrip.roundTripped()).isEqualTo(roundTrip.original());
    }

    @ParameterizedTest
    @MethodSource("supportedVersions")
    void roundTripsThroughKroxylicious(short version) {
        org.apache.kafka.common.message.RequestHeaderData kafka = new org.apache.kafka.common.message.RequestHeaderData();

        FidelityCheck.RoundTrip roundTrip = FidelityCheck.throughKroxylicious(kafka, new RequestHeaderData(), version);

        assertThat(roundTrip.roundTripped()).isEqualTo(roundTrip.original());
    }
}
