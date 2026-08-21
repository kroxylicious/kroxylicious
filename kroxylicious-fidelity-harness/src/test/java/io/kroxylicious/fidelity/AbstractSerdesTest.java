/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared contract tests for a per-namespace RequestHeaderData serdes ({@link KroxyliciousSerdes} /
 * {@link KafkaSerdes}): both must write and read the same wire layout the same way, proven against
 * hand-computed fixtures rather than a self-round-trip. Subclasses supply the namespace-specific message
 * construction/field access; every test here runs once per subclass.
 *
 * @param <M> the namespace's RequestHeaderData type
 */
abstract class AbstractSerdesTest<M> {

    // v2 RequestHeaderData wire layout, computed independently of write()/read():
    // requestApiKey(short)=18, requestApiVersion(short)=7, correlationId(int)=0x01020304,
    // clientId="krox" (length-prefixed), then a v2 tagged-fields count varint of 0.
    static final byte[] KNOWN_BYTES = {
            0x00, 0x12,
            0x00, 0x07,
            0x01, 0x02, 0x03, 0x04,
            0x00, 0x04, 'k', 'r', 'o', 'x',
            0x00
    };

    // Same as KNOWN_BYTES, but the trailing tagged-fields count is 1 instead of 0, followed by one
    // unknown tagged field: tag=1, size=1, data=[0x01].
    static final byte[] KNOWN_BYTES_WITH_UNKNOWN_TAGGED_FIELD = {
            0x00, 0x12,
            0x00, 0x07,
            0x01, 0x02, 0x03, 0x04,
            0x00, 0x04, 'k', 'r', 'o', 'x',
            0x01, 0x01, 0x01, 0x01
    };

    // Same as KNOWN_BYTES, but clientId is null: a NULLABLE_STRING encodes null as a length of -1
    // (short 0xFFFF), with no bytes following for the (absent) string content.
    static final byte[] KNOWN_BYTES_WITH_NULL_CLIENT_ID = {
            0x00, 0x12,
            0x00, 0x07,
            0x01, 0x02, 0x03, 0x04,
            (byte) 0xFF, (byte) 0xFF,
            0x00
    };
    private static final Snapshot NULL_CLIENT_ID_SNAPSHOT = new Snapshot((short) 18, (short) 7, 0x01020304, null, List.of());

    abstract M populate(short requestApiKey, short requestApiVersion, int correlationId, String clientId);

    abstract Snapshot snapshot(M message);

    abstract byte[] write(M message, short version);

    abstract ReadResult<M> read(byte[] bytes, short version);

    @Test
    void shouldWriteKnownByteLayout() {
        // Given
        M message = populate((short) 18, (short) 7, 0x01020304, "krox");

        // When
        byte[] bytes = write(message, (short) 2);

        // Then
        assertThat(bytes).isEqualTo(KNOWN_BYTES);
    }

    @Test
    void shouldDecodeInlineWithSpecifiedVersion() {
        // When
        ReadResult<M> result = read(KNOWN_BYTES, (short) 1);

        // Then
        assertThat(result.error()).isNull();
        // KNOWN_BYTES is v2-shaped (it has a trailing tagged-fields count); reading it as v1 correctly
        // stops before that byte, since v1 has no tagged-fields section - this is exactly the signal
        // assertAllBytesConsumed() relies on to catch a version mismatch.
        assertThat(result.unreadBytes()).isEqualTo(1);
        assertThat(snapshot(result.message())).isEqualTo(new Snapshot((short) 18, (short) 7, 0x01020304, "krox", List.of()));
    }

    @Test
    void shouldReadKnownByteLayoutWithUnknownTaggedField() {
        // When
        ReadResult<M> result = read(KNOWN_BYTES_WITH_UNKNOWN_TAGGED_FIELD, (short) 2);

        // Then
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(snapshot(result.message()))
                .isEqualTo(new Snapshot((short) 18, (short) 7, 0x01020304, "krox", List.of(new TagSnapshot(1, List.of((byte) 1)))));
    }

    @Test
    void shouldWriteNullClientId() {
        // Given
        M message = populate((short) 18, (short) 7, 0x01020304, null);

        // When
        byte[] bytes = write(message, (short) 2);

        // Then
        assertThat(bytes).isEqualTo(KNOWN_BYTES_WITH_NULL_CLIENT_ID);
    }

    @Test
    void shouldReadNullClientId() {
        // When
        ReadResult<M> result = read(KNOWN_BYTES_WITH_NULL_CLIENT_ID, (short) 2);

        // Then
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(snapshot(result.message())).isEqualTo(NULL_CLIENT_ID_SNAPSHOT);
    }

    @Test
    void shouldFailForMalformedInput() {
        // When - dropping the leading byte misaligns every field; requestApiKey/requestApiVersion/
        // correlationId/clientId-length (10 bytes) still decode "successfully" (as garbage), then the
        // claimed clientId length (garbage, from misaligned bytes) vastly exceeds the 4 bytes actually left
        ReadResult<M> result = read(Arrays.copyOfRange(KNOWN_BYTES, 1, KNOWN_BYTES.length), (short) 2);

        // Then
        assertThat(result.unreadBytes()).isEqualTo(4);
        assertThat(result.error()).isInstanceOf(RuntimeException.class).hasMessageContaining("Error reading byte array");
    }

    static List<Byte> toBoxedList(byte[] data) {
        List<Byte> boxed = new ArrayList<>(data.length);
        for (byte b : data) {
            boxed.add(b);
        }
        return boxed;
    }

    record Snapshot(short requestApiKey, short requestApiVersion, int correlationId, String clientId, List<TagSnapshot> tags) {}

    record TagSnapshot(int tag, List<Byte> data) {}
}