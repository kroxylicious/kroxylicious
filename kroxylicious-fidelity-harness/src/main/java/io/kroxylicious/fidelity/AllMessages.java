/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.kafka.common.protocol.ApiMessage;

/**
 * Enumerates every (message, direction, version) combination the Kafka protocol defines, instantiating a
 * fresh Kroxylicious and Kafka instance of each - the "which messages exist" concern shared by every
 * fidelity test that needs to iterate the whole protocol surface.
 */
public final class AllMessages {

    private static final String KAFKA_PACKAGE = "org.apache.kafka.common.message.";
    private static final String KROXYLICIOUS_PACKAGE = "io.kroxylicious.kafka.common.message.";
    private static final String CLASS_NAME_FORMAT = "%s%s%sData";

    private AllMessages() {
    }

    /**
     * A single message type at a single supported protocol version, with a fresh instance of each class
     * family ready to populate.
     *
     * @param messageName the message's base name, e.g. {@code "Heartbeat"}
     * @param direction {@code "Request"}, {@code "Response"}, or {@code ""} for data-only broker APIs
     * @param version the protocol version
     * @param kroxyliciousMessage a fresh Kroxylicious instance of this message/version
     * @param kafkaMessage a fresh Kafka instance of this message/version
     */
    public record VersionedMessage(String messageName, String direction, short version,
                                   ApiMessage kroxyliciousMessage,
                                   org.apache.kafka.common.protocol.ApiMessage kafkaMessage) {

        /**
         * The label existing test suites use to identify this combination in parameterized test output.
         *
         * @return a human-readable label
         */
        public String label() {
            return messageName + direction + " - v" + version;
        }
    }

    /**
     * Streams every (message, direction, version) combination across the client, broker, and controller
     * APIs, each with a freshly instantiated pair of instances.
     *
     * @return the stream of versioned messages
     */
    public static Stream<VersionedMessage> stream() {
        Stream<VersionedMessage> clientApiStream = ApiKeys.clientApis().stream()
                .flatMap(AllMessages::directionalApiStream);
        Stream<VersionedMessage> brokerApiStream = ApiKeys.brokerApis().stream()
                .filter(apiKeys -> !ApiKeys.clientApis().contains(apiKeys))
                .flatMap(AllMessages::dataOnlyApiStream);
        Stream<VersionedMessage> controllerApiStream = ApiKeys.controllerApis().stream()
                .filter(apiKeys -> !ApiKeys.clientApis().contains(apiKeys))
                .flatMap(AllMessages::directionalApiStream);

        return Stream.concat(clientApiStream, Stream.concat(brokerApiStream, controllerApiStream));
    }

    private static Stream<VersionedMessage> directionalApiStream(ApiKeys apiKey) {
        String messageName = apiKeyToMessageName(apiKey);
        return Stream.of("Request", "Response")
                .flatMap(direction -> versionedMessageStream(messageName, direction));
    }

    private static Stream<VersionedMessage> dataOnlyApiStream(ApiKeys apiKey) {
        String messageName = apiKeyToMessageName(apiKey);
        return versionedMessageStream(messageName, "");
    }

    private static String apiKeyToMessageName(ApiKeys apiKey) {
        String messageName;
        if (apiKey.name().contains("_")) {
            messageName = Arrays.stream(apiKey.name().split("_"))
                    .map(AllMessages::capitalizeFirst)
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

    private static Stream<VersionedMessage> versionedMessageStream(String messageName, String direction) {

        org.apache.kafka.common.protocol.ApiMessage kafkaMessage = kafkaMessage(messageName, direction);
        short lowest = kafkaMessage.lowestSupportedVersion();
        short highest = kafkaMessage.highestSupportedVersion();

        // Ensure full isolation by creating message instances per version
        return IntStream.rangeClosed(lowest, highest)
                .mapToObj(version -> new VersionedMessage(messageName, direction, (short) version,
                        kroxyliciousMessage(messageName, direction), kafkaMessage(messageName, direction)));
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
