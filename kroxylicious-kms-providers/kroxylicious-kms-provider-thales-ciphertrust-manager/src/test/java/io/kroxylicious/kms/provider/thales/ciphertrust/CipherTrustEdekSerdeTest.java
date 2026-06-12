/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kms.service.Serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CipherTrustEdekSerdeTest {

    private static final String KEY_ID = "test-key-id";
    private final Serde<CipherTrustEdek> serde = CipherTrustEdekSerde.instance();

    @Test
    void shouldRoundTrip() {
        var edek = new CipherTrustEdek(KEY_ID, new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5 }, 1, "gcm", new byte[]{ 6, 7, 8 });
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        buf.flip();
        var deserialized = serde.deserialize(buf);
        assertThat(deserialized).isEqualTo(edek);
    }

    @Test
    void sizeOf() {
        var edek = new CipherTrustEdek(KEY_ID, new byte[]{ 1 }, new byte[]{ 2 }, 1, "gcm", new byte[]{ 3 });
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        assertThat(buf.remaining()).isZero();
    }

    @Test
    void roundTripWithVariableLengthStrings() {
        var longId = "a".repeat(100);
        var longMode = "aes-256-gcm-mode";
        var edek = new CipherTrustEdek(longId, new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5 }, 10, longMode, new byte[]{ 6, 7, 8, 9, 10, 11, 12 });
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        buf.flip();
        var deserialized = serde.deserialize(buf);
        assertThat(deserialized).isEqualTo(edek);
    }

    @Test
    void roundTripWithUnicodeCharacters() {
        var unicodeId = "key-🔑-identifier";
        var edek = new CipherTrustEdek(unicodeId, new byte[]{ 1, 2 }, new byte[]{ 3 }, 1, "gcm", new byte[]{ 4 });
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        buf.flip();
        var deserialized = serde.deserialize(buf);
        assertThat(deserialized).isEqualTo(edek);
    }

    static Stream<Arguments> deserializeErrors() {
        return Stream.of(
                Arguments.of("empty buffer", new byte[]{}),
                Arguments.of("only id length", new byte[]{ 1 }),
                Arguments.of("incomplete id", new byte[]{ 3, 'A', 'B' }),
                Arguments.of("no version", new byte[]{ 2, 'A', 'B' }),
                Arguments.of("no mode length", new byte[]{ 2, 'A', 'B', 1 }),
                Arguments.of("incomplete mode", new byte[]{ 2, 'A', 'B', 1, 3, 'g', 'c' }),
                Arguments.of("no ciphertext length", new byte[]{ 2, 'A', 'B', 1, 3, 'g', 'c', 'm' }),
                Arguments.of("incomplete ciphertext", new byte[]{ 2, 'A', 'B', 1, 3, 'g', 'c', 'm', 2, 1 }),
                Arguments.of("no tag length", new byte[]{ 2, 'A', 'B', 1, 3, 'g', 'c', 'm', 1, 99 }),
                Arguments.of("incomplete tag", new byte[]{ 2, 'A', 'B', 1, 3, 'g', 'c', 'm', 1, 99, 2, 1 }),
                Arguments.of("no iv length", new byte[]{ 2, 'A', 'B', 1, 3, 'g', 'c', 'm', 1, 99, 1, 88 }),
                Arguments.of("incomplete iv", new byte[]{ 2, 'A', 'B', 1, 3, 'g', 'c', 'm', 1, 99, 1, 88, 2, 1 }));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void deserializeErrors(String name, byte[] serializedBytes) {
        var buf = ByteBuffer.wrap(serializedBytes);
        assertThatThrownBy(() -> serde.deserialize(buf))
                .isInstanceOf(RuntimeException.class); // Can be IllegalArgumentException, BufferUnderflowException, or StringIndexOutOfBoundsException
    }

}
