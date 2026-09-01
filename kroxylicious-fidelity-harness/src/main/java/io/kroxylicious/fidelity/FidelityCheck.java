/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves wire-level fidelity between a {@code io.kroxylicious.kafka.common.message.*Data} instance and
 * its {@code org.apache.kafka.common.message.*Data} equivalent, without either side needing to know
 * anything about the other's API shape.
 * <p>
 * The properties this proves are read-correctness, not round-trip byte equality: can Kroxylicious
 * correctly decode a message Kafka serialized ({@link #kroxyliciousReads}), can Kafka correctly decode a
 * message Kroxylicious serialized ({@link #kafkaReads}), and given the same invalid bytes, does one side
 * fail the same way the other does ({@link #compareErrorHandling}). A re-encode-and-diff round trip was
 * tried first and rejected: it can't distinguish "the other side misread these bytes" from "the other side
 * read them fine but re-encodes differently," since both produce the same symptom.
 */
@SuppressWarnings("java:S5960") // This module is intended for users to write tests with, using AssertJ here is a conscious and deliberate choice
public final class FidelityCheck {

    private FidelityCheck() {
    }

    /**
     * Writes {@code kafkaSource} with the Kafka codec, then decodes those bytes with the Kroxylicious
     * codec using {@code oursScratch} as scratch space.
     *
     * @param kafkaSource the Kafka-side instance to write
     * @param oursScratch a fresh Kroxylicious-side instance to decode into
     * @param version the protocol version to serialize/deserialize at
     * @param <T> the Kroxylicious-side message type
     * @return the result of decoding {@code kafkaSource}'s bytes with the Kroxylicious codec
     */
    public static <T extends io.kroxylicious.kafka.common.protocol.Message> ReadResult<T> kroxyliciousReads(
                                                                                                            io.kroxylicious.kafka.common.protocol.Message kafkaSource,
                                                                                                            T oursScratch,
                                                                                                            short version) {
        byte[] kafkaBytes = KafkaSerdes.write(kafkaSource, version);
        return KroxyliciousSerdes.read(oursScratch, kafkaBytes, version);
    }

    /**
     * Writes {@code oursSource} with the Kroxylicious codec, then decodes those bytes with the Kafka
     * codec using {@code kafkaScratch} as scratch space.
     *
     * @param oursSource the Kroxylicious-side instance to write
     * @param kafkaScratch a fresh Kafka-side instance to decode into
     * @param version the protocol version to serialize/deserialize at
     * @param <T> the Kafka-side message type
     * @return the result of decoding {@code oursSource}'s bytes with the Kafka codec
     */
    public static <T extends io.kroxylicious.kafka.common.protocol.Message> ReadResult<T> kafkaReads(
                                                                                                io.kroxylicious.kafka.common.protocol.Message oursSource,
                                                                                                T kafkaScratch,
                                                                                                short version) {
        byte[] oursBytes = KroxyliciousSerdes.write(oursSource, version);
        return KafkaSerdes.read(kafkaScratch, oursBytes, version);
    }

    /**
     * Feeds the same (presumably invalid) bytes to both codecs' {@code read()} and reports whether they
     * failed equivalently.
     *
     * @param bytes the bytes to decode with both codecs
     * @param kafkaScratch a fresh Kafka-side instance to decode into
     * @param oursScratch a fresh Kroxylicious-side instance to decode into
     * @param version the protocol version to deserialize at
     * @param <K> the Kafka-side message type
     * @param <X> the Kroxylicious-side message type
     * @return the two sides' decode errors, if any
     */
    public static <K extends io.kroxylicious.kafka.common.protocol.Message, X extends io.kroxylicious.kafka.common.protocol.Message> ErrorParity compareErrorHandling(
                                                                                                                                                                 byte[] bytes,
                                                                                                                                                                 K kafkaScratch,
                                                                                                                                                                 X oursScratch,
                                                                                                                                                                 short version) {
        Throwable kafkaError = KafkaSerdes.read(kafkaScratch, bytes, version).error();
        Throwable kroxyliciousError = KroxyliciousSerdes.read(oursScratch, bytes, version).error();
        return new ErrorParity(kafkaError, kroxyliciousError);
    }

    /**
     * The decode error (if any) each codec produced for the same bytes.
     *
     * @param kafkaError the exception thrown by the Kafka codec, or {@code null} if it decoded successfully
     * @param kroxyliciousError the exception thrown by the Kroxylicious codec, or {@code null} if it decoded successfully
     */
    public record ErrorParity(Throwable kafkaError, Throwable kroxyliciousError) {

        /**
         * Asserts that either both codecs failed to decode, or both succeeded.
         *
         * @throws AssertionError if exactly one codec failed
         */
        public void assertEquivalentResults() {
            if (kafkaError == null) {
                assertThat(kroxyliciousError).isNull();
            }
            else {
                assertThat(kroxyliciousError)
                        .hasSameClassAs(kafkaError)
                        .hasMessage(kafkaError.getMessage());
            }
        }
    }
}
