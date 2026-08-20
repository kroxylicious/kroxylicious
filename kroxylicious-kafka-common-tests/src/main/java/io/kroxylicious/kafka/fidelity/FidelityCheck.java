/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kafka.fidelity;

/**
 * Proves wire-level fidelity between a {@code io.kroxylicious.kafka.common.message.*Data} instance and
 * its {@code org.apache.kafka.common.message.*Data} equivalent, without either side needing to know
 * anything about the other's API shape.
 * <p>
 * The only contract exercised is the wire format itself: a message is written with its own codec, the
 * resulting bytes are read into a fresh instance of the other side using that side's own codec, and the
 * fresh instance is written back out. Callers compare {@link RoundTrip#original()} against
 * {@link RoundTrip#roundTripped()} - equal bytes mean the two implementations are wire-compatible at the
 * given version.
 */
public final class FidelityCheck {

    private FidelityCheck() {
    }

    /**
     * Writes {@code ours} with the Kroxylicious codec, then decodes and re-encodes those bytes with the
     * Kafka codec using {@code kafkaScratch} as scratch space.
     *
     * @param ours the Kroxylicious-side instance to write
     * @param kafkaScratch a fresh Kafka-side instance used as decode/re-encode scratch space
     * @param version the protocol version to serialize at
     * @return the original bytes and the bytes produced by the Kafka codec's round trip
     */
    public static RoundTrip throughKafka(io.kroxylicious.kafka.common.protocol.Message ours,
                                         org.apache.kafka.common.protocol.Message kafkaScratch,
                                         short version) {
        byte[] original = KroxyliciousSerdes.write(ours, version);
        KafkaSerdes.read(kafkaScratch, original, version);
        byte[] roundTripped = KafkaSerdes.write(kafkaScratch, version);
        return new RoundTrip(original, roundTripped);
    }

    /**
     * Writes {@code kafka} with the Kafka codec, then decodes and re-encodes those bytes with the
     * Kroxylicious codec using {@code oursScratch} as scratch space.
     *
     * @param kafka the Kafka-side instance to write
     * @param oursScratch a fresh Kroxylicious-side instance used as decode/re-encode scratch space
     * @param version the protocol version to serialize at
     * @return the original bytes and the bytes produced by the Kroxylicious codec's round trip
     */
    public static RoundTrip throughKroxylicious(org.apache.kafka.common.protocol.Message kafka,
                                                io.kroxylicious.kafka.common.protocol.Message oursScratch,
                                                short version) {
        byte[] original = KafkaSerdes.write(kafka, version);
        KroxyliciousSerdes.read(oursScratch, original, version);
        byte[] roundTripped = KroxyliciousSerdes.write(oursScratch, version);
        return new RoundTrip(original, roundTripped);
    }

    /**
     * The bytes originally written, and the bytes produced by decoding and re-encoding them with the
     * other implementation. Equal arrays mean the two implementations are wire-compatible.
     *
     * @param original bytes as written by the originating side's own codec
     * @param roundTripped bytes produced by decoding {@code original} and re-encoding with the other side's codec
     */
    public record RoundTrip(byte[] original, byte[] roundTripped) {}
}