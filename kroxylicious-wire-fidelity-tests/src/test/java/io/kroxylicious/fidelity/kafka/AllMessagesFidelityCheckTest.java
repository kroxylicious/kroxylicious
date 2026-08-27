/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.fidelity.kafka;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.fidelity.FidelityCheck;
import io.kroxylicious.fidelity.ReadResult;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

import static org.assertj.core.api.Assertions.assertThat;

class AllMessagesFidelityCheckTest {

    private static final String KAFKA_PACKAGE = "org.apache.kafka.common.message.";
    private static final String KROXYLICIOUS_PACKAGE = "io.kroxylicious.kafka.common.message.";
    private static final String CLASS_NAME_FORMAT = "%s%s%sData";

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

    static Stream<Arguments> allMessageVersions() {
        Stream<Arguments> clientApiStream = ApiKeys.clientApis().stream()
                .flatMap(AllMessagesFidelityCheckTest::directionalApiStream);
        Stream<Arguments> brokerApiStream = ApiKeys.brokerApis().stream()
                .filter(apiKeys -> !ApiKeys.clientApis().contains(apiKeys))
                .flatMap(AllMessagesFidelityCheckTest::dataOnlyApiStream);
        Stream<Arguments> controllerApiStream = ApiKeys.controllerApis().stream()
                .filter(apiKeys -> !ApiKeys.clientApis().contains(apiKeys))
                .flatMap(AllMessagesFidelityCheckTest::directionalApiStream);

        return Stream.concat(clientApiStream, Stream.concat(brokerApiStream, controllerApiStream));
    }

    private static Stream<Arguments> directionalApiStream(ApiKeys apiKey) {
        String messageName = apiKeyToMessageName(apiKey);
        return Stream.of("Request", "Response")
                .flatMap(direction -> versionedMessageStream(messageName, direction));
    }

    private static Stream<Arguments> dataOnlyApiStream(ApiKeys apiKey) {
        String messageName = apiKeyToMessageName(apiKey);
        return versionedMessageStream(messageName, "");
    }

    private static String apiKeyToMessageName(ApiKeys apiKey) {
        String messageName;
        if (apiKey.name().contains("_")) {
            messageName = Arrays.stream(apiKey.name().split("_"))
                    .map(AllMessagesFidelityCheckTest::capitalizeFirst)
                    .collect(Collectors.joining(""));
        }
        else {
            messageName = capitalizeFirst(apiKey.name());
        }
        return messageName;
    }

    private static String capitalizeFirst(String word) {
        String result;
        if (word.isEmpty()) {
            result = word;
        }
        else {
            String lowerCase = word.toLowerCase(Locale.ROOT);
            result = lowerCase.substring(0, 1).toUpperCase(Locale.ENGLISH) +
                    lowerCase.substring(1);
        }
        return result;
    }

    private static Stream<Arguments> versionedMessageStream(String messageName, String direction) {

        org.apache.kafka.common.protocol.ApiMessage kafkaMessage = kafkaMessage(messageName, direction);
        short lowest = kafkaMessage.lowestSupportedVersion();
        short highest = kafkaMessage.highestSupportedVersion();

        // Ensure full isolation by creating message instances per argumentSet
        return IntStream.rangeClosed(lowest, highest)
                .mapToObj(version -> Arguments.argumentSet(messageName + direction + " - v" + version,
                        (short) version,
                        kroxyliciousMessage(messageName, direction),
                        kafkaMessage(messageName, direction)));
    }

    private static org.apache.kafka.common.protocol.ApiMessage kafkaMessage(String messageName, String direction) {
        try {
            Class<?> kafkaClass = loadClass(KAFKA_PACKAGE, messageName, direction);
            return kafkaMessage(kafkaClass);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static ApiMessage kroxyliciousMessage(String messageName, String direction) {
        try {
            Class<?> kroxyliciousClass = loadClass(KROXYLICIOUS_PACKAGE, messageName, direction);
            try {
                return (ApiMessage) kroxyliciousClass.getDeclaredConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to instantiate " + kroxyliciousClass, e);
            }
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static Class<?> loadClass(String packageName, String messageName, String direction) throws ClassNotFoundException {
        String className = CLASS_NAME_FORMAT.formatted(packageName, messageName, direction);
        return Class.forName(className);
    }

    private static org.apache.kafka.common.protocol.ApiMessage kafkaMessage(Class<?> kafkaClass) {
        try {
            return (org.apache.kafka.common.protocol.ApiMessage) kafkaClass.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to instantiate " + kafkaClass, e);
        }
    }

}