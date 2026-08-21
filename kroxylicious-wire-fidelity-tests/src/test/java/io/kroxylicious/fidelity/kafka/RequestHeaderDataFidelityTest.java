/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.kafka;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.fidelity.FidelityCheck;
import io.kroxylicious.fidelity.ReadResult;
import io.kroxylicious.kafka.common.message.RequestHeaderData;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@link RequestHeaderData} can be read correctly by, and can correctly read messages from,
 * {@code org.apache.kafka.common.message.RequestHeaderData}, at every version it supports. This is the
 * first spec proven by the fidelity harness; {@code AllDataClassesFidelityTest} generalizes this same
 * check across every spec.
 */
class RequestHeaderDataFidelityTest {

    static Stream<Short> supportedVersions() {
        RequestHeaderData reference = new RequestHeaderData();
        return IntStream.rangeClosed(reference.lowestSupportedVersion(), reference.highestSupportedVersion())
                .mapToObj(version -> (short) version);
    }

    @ParameterizedTest
    @MethodSource("supportedVersions")
    void kroxyliciousShouldReadKafkaSerialisedMessage(short version) {
        // Given
        org.apache.kafka.common.message.RequestHeaderData kafkaSource = new org.apache.kafka.common.message.RequestHeaderData()
                .setRequestApiKey((short) 18)
                .setRequestApiVersion((short) 7)
                .setCorrelationId(0x01020304)
                .setClientId("kroxylicious-client");

        // When
        ReadResult<RequestHeaderData> result = FidelityCheck.kroxyliciousReads(kafkaSource, new RequestHeaderData(), version);

        // Then
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(result.message().requestApiKey()).isEqualTo((short) 18);
        assertThat(result.message().requestApiVersion()).isEqualTo((short) 7);
        assertThat(result.message().correlationId()).isEqualTo(0x01020304);
        assertThat(result.message().clientId()).isEqualTo("kroxylicious-client");
    }

    @ParameterizedTest
    @MethodSource("supportedVersions")
    void kafkaShouldReadKroxyliciousSerialisedMessage(short version) {
        // Given
        RequestHeaderData oursSource = new RequestHeaderData()
                .setRequestApiKey((short) 18)
                .setRequestApiVersion((short) 7)
                .setCorrelationId(0x01020304)
                .setClientId("kroxylicious-client");

        // When
        ReadResult<org.apache.kafka.common.message.RequestHeaderData> result = FidelityCheck.kafkaReads(
                oursSource, new org.apache.kafka.common.message.RequestHeaderData(), version);

        // Then
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(result.message().requestApiKey()).isEqualTo((short) 18);
        assertThat(result.message().requestApiVersion()).isEqualTo((short) 7);
        assertThat(result.message().correlationId()).isEqualTo(0x01020304);
        assertThat(result.message().clientId()).isEqualTo("kroxylicious-client");
    }
}
