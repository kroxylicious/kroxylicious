/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.fidelity.kafka;

import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import org.apache.kafka.common.protocol.types.BoundField;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.fidelity.AllMessages;
import io.kroxylicious.fidelity.FidelityCheck;
import io.kroxylicious.fidelity.ReadResult;
import io.kroxylicious.fidelity.populate.NullFieldStrategy;
import io.kroxylicious.fidelity.populate.NullableFieldEnumerator;
import io.kroxylicious.fidelity.populate.PopulationResult;
import io.kroxylicious.fidelity.populate.SchemaDrivenMessagePopulator;
import io.kroxylicious.fidelity.populate.ValidScalarStrategy;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that a message round-trips correctly when exactly one of its top-level nullable fields is set to
 * {@code null} and every other field carries a valid, populated value - complementing
 * {@link AllMessagesFidelityCheckTest}'s all-fields-populated and all-fields-default checks.
 */
class NullableFieldFidelityTest {

    @ParameterizedTest
    @MethodSource("allNullableFieldVersions")
    void kroxyliciousShouldReadKafkaSerialisedMessageWithFieldNulled(short version, ApiMessage kroxyliciousMessage,
                                                                     org.apache.kafka.common.protocol.ApiMessage kafkaMessage, BoundField nullableField) {
        // Given
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(
                new NullFieldStrategy(nullableField, new ValidScalarStrategy(new Random(42L))));
        PopulationResult populated = populator.populate(kafkaMessage, version);

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
    @MethodSource("allNullableFieldVersions")
    void kafkaShouldReadKroxyliciousSerialisedMessageWithFieldNulled(short version, ApiMessage kroxyliciousMessage,
                                                                     org.apache.kafka.common.protocol.ApiMessage kafkaMessage, BoundField nullableField) {
        // Given
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(
                new NullFieldStrategy(nullableField, new ValidScalarStrategy(new Random(42L))));
        PopulationResult populated = populator.populate(kroxyliciousMessage, version);

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

    static Stream<Arguments> allNullableFieldVersions() {
        return AllMessages.stream().flatMap(NullableFieldFidelityTest::nullableFieldArguments);
    }

    /**
     * Every nullable field of a given message/version gets its own argument set, each with its own fresh
     * pair of instances so populating one field's argument set can never leak state into another's.
     */
    private static Stream<Arguments> nullableFieldArguments(AllMessages.VersionedMessage versionedMessage) {
        List<BoundField> nullableFields = NullableFieldEnumerator.topLevelNullableFields(versionedMessage.kafkaMessage(), versionedMessage.version());
        return nullableFields.stream()
                .map(field -> Arguments.argumentSet(versionedMessage.label() + " - null " + field.def.name,
                        versionedMessage.version(),
                        freshInstance(versionedMessage.kroxyliciousMessage()),
                        freshInstance(versionedMessage.kafkaMessage()),
                        field));
    }

    @SuppressWarnings("unchecked")
    private static <T> T freshInstance(T template) {
        try {
            return (T) template.getClass().getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to instantiate " + template.getClass(), e);
        }
    }
}
