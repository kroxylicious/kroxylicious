/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.fidelity.kafka;

import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kafka.message.json.KafkaApiMessageConverter;
import io.kroxylicious.kafka.message.json.VendoredKafkaApiMessageConverter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Confirms that {@link VendoredKafkaApiMessageConverter} produces the same JSON representation as
 * {@link KafkaApiMessageConverter} for a default-constructed instance of every message type shared
 * between {@code org.apache.kafka.common.message.ApiMessageType} and its vendored equivalent, at
 * every version supported by both.
 */
class JsonConverterFidelityTest {

    private record TypePair(org.apache.kafka.common.message.ApiMessageType kafkaType, io.kroxylicious.kafka.common.message.ApiMessageType vendoredType) {}

    @ParameterizedTest
    @MethodSource("allRequestVersions")
    void requestJsonMatchesKafka(TypePair types, short version) {
        // Given

        // When
        var kafkaJson = KafkaApiMessageConverter.requestConverterFor(types.kafkaType()).writer().apply(types.kafkaType().newRequest(), version);
        var vendoredJson = VendoredKafkaApiMessageConverter.requestConverterFor(types.vendoredType()).writer().apply(types.vendoredType().newRequest(), version);

        // Then
        assertThat(vendoredJson).isEqualTo(kafkaJson);
    }

    @ParameterizedTest
    @MethodSource("allResponseVersions")
    void responseJsonMatchesKafka(TypePair types, short version) {
        // Given

        // When
        var kafkaJson = KafkaApiMessageConverter.responseConverterFor(types.kafkaType()).writer().apply(types.kafkaType().newResponse(), version);
        var vendoredJson = VendoredKafkaApiMessageConverter.responseConverterFor(types.vendoredType()).writer().apply(types.vendoredType().newResponse(), version);

        // Then
        assertThat(vendoredJson).isEqualTo(kafkaJson);
    }

    static Stream<Arguments> allRequestVersions() {
        return sharedApiMessageTypes().flatMap(types -> versionsOf(types).mapToObj(
                version -> Arguments.argumentSet(types.kafkaType().name() + "Request - v" + version, types, (short) version)));
    }

    static Stream<Arguments> allResponseVersions() {
        return sharedApiMessageTypes().flatMap(types -> versionsOf(types).mapToObj(
                version -> Arguments.argumentSet(types.kafkaType().name() + "Response - v" + version, types, (short) version)));
    }

    private static IntStream versionsOf(TypePair types) {
        return IntStream.rangeClosed(types.kafkaType().lowestSupportedVersion(), types.kafkaType().highestSupportedVersion(true));
    }

    private static Stream<TypePair> sharedApiMessageTypes() {
        return Stream.of(org.apache.kafka.common.message.ApiMessageType.values())
                .flatMap(kafkaType -> vendoredEquivalent(kafkaType).map(vendoredType -> new TypePair(kafkaType, vendoredType)).stream());
    }

    private static Optional<io.kroxylicious.kafka.common.message.ApiMessageType> vendoredEquivalent(org.apache.kafka.common.message.ApiMessageType kafkaType) {
        try {
            return Optional.of(io.kroxylicious.kafka.common.message.ApiMessageType.valueOf(kafkaType.name()));
        }
        catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
