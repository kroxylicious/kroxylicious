/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.fidelity.kafka;

import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.fidelity.AllMessages;
import io.kroxylicious.fidelity.FidelityCheck;
import io.kroxylicious.fidelity.ReadResult;
import io.kroxylicious.fidelity.populate.PopulationResult;
import io.kroxylicious.fidelity.populate.SchemaDrivenMessagePopulator;
import io.kroxylicious.fidelity.populate.ValidScalarStrategy;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

import static org.assertj.core.api.Assertions.assertThat;

class AllMessagesFidelityCheckTest {

    private SchemaDrivenMessagePopulator validScalarMessagePopulator;

    @BeforeEach
    void setUp() {
        validScalarMessagePopulator = new SchemaDrivenMessagePopulator(new ValidScalarStrategy(new Random(42L)));
    }

    @ParameterizedTest
    @MethodSource("allMessageVersions")
    void kroxyliciousShouldReadEmptyKafkaSerialisedMessage(short version, ApiMessage kroxyliciousMessage, org.apache.kafka.common.protocol.ApiMessage kafkaMessage) {
        // Given

        // When
        ReadResult<?> result = FidelityCheck.kroxyliciousReads(
                kafkaMessage,
                (io.kroxylicious.kafka.common.protocol.Message) kroxyliciousMessage,
                version);

        // Then
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(result.message()).usingRecursiveComparison().isEqualTo(kafkaMessage);
    }

    @ParameterizedTest
    @MethodSource("allMessageVersions")
    void kafkaShouldReadEmptyKroxyliciousSerialisedMessage(short version,
                                                           ApiMessage kroxyliciousMessage, org.apache.kafka.common.protocol.ApiMessage kafkaMessage) {
        // Given

        // When
        ReadResult<?> result = FidelityCheck.kafkaReads(
                kroxyliciousMessage,
                (org.apache.kafka.common.protocol.Message) kafkaMessage,
                version);

        // Then
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(result.message()).usingRecursiveComparison().isEqualTo(kroxyliciousMessage);
    }

    @ParameterizedTest
    @MethodSource("allMessageVersions")
    void kroxyliciousShouldReadValidPopulatedKafkaSerialisedMessage(short version, ApiMessage kroxyliciousMessage,
                                                                    org.apache.kafka.common.protocol.ApiMessage kafkaMessage) {
        // Given
        PopulationResult populated = validScalarMessagePopulator.populate(kafkaMessage, version);

        // When
        ReadResult<?> result = FidelityCheck.kroxyliciousReads(
                kafkaMessage,
                (io.kroxylicious.kafka.common.protocol.Message) kroxyliciousMessage,
                version);

        // Then
        assertThat(populated).isInstanceOf(PopulationResult.Populated.class);
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(result.message()).usingRecursiveComparison().isEqualTo(kafkaMessage);
    }

    @ParameterizedTest
    @MethodSource("allMessageVersions")
    void kafkaShouldReadValidPopulatedKroxyliciousSerialisedMessage(short version,
                                                                    ApiMessage kroxyliciousMessage, org.apache.kafka.common.protocol.ApiMessage kafkaMessage) {
        // Given
        PopulationResult populated = validScalarMessagePopulator.populate(kroxyliciousMessage, version);

        // When
        ReadResult<?> result = FidelityCheck.kafkaReads(
                kroxyliciousMessage,
                (org.apache.kafka.common.protocol.Message) kafkaMessage,
                version);

        // Then
        assertThat(populated).isInstanceOf(PopulationResult.Populated.class);
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(result.message()).usingRecursiveComparison().isEqualTo(kroxyliciousMessage);
    }

    static Stream<Arguments> allMessageVersions() {
        return AllMessages.stream()
                .map(versionedMessage -> Arguments.argumentSet(versionedMessage.label(),
                        versionedMessage.version(),
                        versionedMessage.kroxyliciousMessage(),
                        versionedMessage.kafkaMessage()));
    }

}