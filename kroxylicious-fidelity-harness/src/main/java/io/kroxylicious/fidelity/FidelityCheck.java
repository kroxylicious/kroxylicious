/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.util.Arrays;

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
        ReadResult<org.apache.kafka.common.protocol.Message> decoded = KafkaSerdes.read(kafkaScratch, original, version);
        byte[] roundTripped = KafkaSerdes.write(decoded.message(), version);
        return new RoundTrip(original, roundTripped, decoded.unreadBytes());
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
        ReadResult<io.kroxylicious.kafka.common.protocol.Message> decoded = KroxyliciousSerdes.read(oursScratch, original, version);
        byte[] roundTripped = KroxyliciousSerdes.write(decoded.message(), version);
        return new RoundTrip(original, roundTripped, decoded.unreadBytes());
    }

    /**
     * The bytes originally written, and the bytes produced by decoding and re-encoding them with the
     * other implementation. Equal arrays mean the two implementations are wire-compatible.
     * <p>
     * Defensively copies its array components on construction and on read, and defines
     * {@code equals}/{@code hashCode}/{@code toString} in terms of array contents rather than identity.
     *
     * @param original bytes as written by the originating side's own codec
     * @param roundTripped bytes produced by decoding {@code original} and re-encoding with the other side's codec
     * @param unreadBytes bytes of {@code original} left unconsumed when decoding it back - non-zero means
     *     the decode was incomplete, even when {@code original} and {@code roundTripped} happen to match
     */
    @SuppressWarnings("ArrayRecordComponent") // arrays are cloned, and equals/hashCode is overridden => safe
    public record RoundTrip(byte[] original, byte[] roundTripped, int unreadBytes) {

        /**
         * Defensively clones {@code original} and {@code roundTripped} so this instance is independent of
         * the caller's arrays.
         */
        public RoundTrip {
            original = original.clone();
            roundTripped = roundTripped.clone();
        }

        /**
         * Asserts that decoding {@code original} consumed every byte. Intended for use from test code, e.g.
         * {@code assertThat(roundTrip).satisfies(RoundTrip::assertAllBytesConsumed)}.
         *
         * @throws AssertionError if any bytes of {@code original} were left unconsumed
         */
        public void assertAllBytesConsumed() {
            if (unreadBytes != 0) {
                throw new AssertionError("Decoding left " + unreadBytes + " byte(s) unconsumed");
            }
        }

        /**
         * A copy of the original byte array
         * @return a clone of the originally written bytes
         */
        @Override
        public byte[] original() {
            return original.clone();
        }

        /**
         * A copy of the bytes after being re-serialised.
         * @return a clone of the bytes produced by the round trip
         */
        @Override
        public byte[] roundTripped() {
            return roundTripped.clone();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof RoundTrip other)) {
                return false;
            }
            return unreadBytes == other.unreadBytes
                    && Arrays.equals(original, other.original)
                    && Arrays.equals(roundTripped, other.roundTripped);
        }

        @Override
        public int hashCode() {
            return (31 * Arrays.hashCode(original) + Arrays.hashCode(roundTripped)) * 31 + unreadBytes;
        }

        @Override
        public String toString() {
            return "RoundTrip[original=" + Arrays.toString(original) + ", roundTripped=" + Arrays.toString(roundTripped)
                    + ", unreadBytes=" + unreadBytes + "]";
        }
    }
}