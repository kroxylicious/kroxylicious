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

    @Test
    void shouldRejectNullId() {
        assertThatThrownBy(() -> new CipherTrustEdek(null, new byte[]{ 1 }, new byte[]{ 2 }, 1, "gcm", new byte[]{ 3 }))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("id cannot be null");
    }

    @Test
    void shouldRejectEmptyId() {
        assertThatThrownBy(() -> new CipherTrustEdek("", new byte[]{ 1 }, new byte[]{ 2 }, 1, "gcm", new byte[]{ 3 }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("id cannot be empty");
    }

    @Test
    void shouldRejectNullCiphertext() {
        assertThatThrownBy(() -> new CipherTrustEdek(KEY_ID, null, new byte[]{ 2 }, 1, "gcm", new byte[]{ 3 }))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("ciphertext cannot be null");
    }

    @Test
    void shouldRejectEmptyCiphertext() {
        assertThatThrownBy(() -> new CipherTrustEdek(KEY_ID, new byte[]{}, new byte[]{ 2 }, 1, "gcm", new byte[]{ 3 }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ciphertext cannot be empty");
    }

    @Test
    void shouldRejectNullTag() {
        assertThatThrownBy(() -> new CipherTrustEdek(KEY_ID, new byte[]{ 1 }, null, 1, "gcm", new byte[]{ 3 }))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("tag cannot be null");
    }

    @Test
    void shouldRejectEmptyTag() {
        assertThatThrownBy(() -> new CipherTrustEdek(KEY_ID, new byte[]{ 1 }, new byte[]{}, 1, "gcm", new byte[]{ 3 }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tag cannot be empty");
    }

    @Test
    void shouldRejectNullMode() {
        assertThatThrownBy(() -> new CipherTrustEdek(KEY_ID, new byte[]{ 1 }, new byte[]{ 2 }, 1, null, new byte[]{ 3 }))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("mode cannot be null");
    }

    @Test
    void shouldRejectEmptyMode() {
        assertThatThrownBy(() -> new CipherTrustEdek(KEY_ID, new byte[]{ 1 }, new byte[]{ 2 }, 1, "", new byte[]{ 3 }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mode cannot be empty");
    }

    @Test
    void shouldRejectNullIv() {
        assertThatThrownBy(() -> new CipherTrustEdek(KEY_ID, new byte[]{ 1 }, new byte[]{ 2 }, 1, "gcm", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("iv cannot be null");
    }

    @Test
    void shouldRejectEmptyIv() {
        assertThatThrownBy(() -> new CipherTrustEdek(KEY_ID, new byte[]{ 1 }, new byte[]{ 2 }, 1, "gcm", new byte[]{}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("iv cannot be empty");
    }

    @Test
    void shouldImplementEqualsCorrectly() {
        var edek1 = new CipherTrustEdek(KEY_ID, new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5 }, 1, "gcm", new byte[]{ 6, 7, 8 });
        var edek2 = new CipherTrustEdek(KEY_ID, new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5 }, 1, "gcm", new byte[]{ 6, 7, 8 });

        assertThat(edek1)
                .isEqualTo(edek2)
                .hasSameHashCodeAs(edek1)
                .isNotEqualTo(null)
                .isNotEqualTo("not an edek");
    }

    @ParameterizedTest
    @MethodSource("notEqualEdeks")
    void shouldNotBeEqualToDifferentEdeks(CipherTrustEdek other) {
        var edek = new CipherTrustEdek(KEY_ID, new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5 }, 1, "gcm", new byte[]{ 6, 7, 8 });
        assertThat(edek).isNotEqualTo(other);
    }

    static Stream<Arguments> notEqualEdeks() {
        return Stream.of(
                Arguments.argumentSet("different id", new CipherTrustEdek("different", new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5 }, 1, "gcm", new byte[]{ 6, 7, 8 })),
                Arguments.argumentSet("different ciphertext", new CipherTrustEdek(KEY_ID, new byte[]{ 9 }, new byte[]{ 4, 5 }, 1, "gcm", new byte[]{ 6, 7, 8 })),
                Arguments.argumentSet("different tag", new CipherTrustEdek(KEY_ID, new byte[]{ 1, 2, 3 }, new byte[]{ 9 }, 1, "gcm", new byte[]{ 6, 7, 8 })),
                Arguments.argumentSet("different version", new CipherTrustEdek(KEY_ID, new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5 }, 2, "gcm", new byte[]{ 6, 7, 8 })),
                Arguments.argumentSet("different mode", new CipherTrustEdek(KEY_ID, new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5 }, 1, "cbc", new byte[]{ 6, 7, 8 })),
                Arguments.argumentSet("different iv", new CipherTrustEdek(KEY_ID, new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5 }, 1, "gcm", new byte[]{ 9 })));
    }

    @Test
    void shouldImplementHashCodeCorrectly() {
        var edek1 = new CipherTrustEdek(KEY_ID, new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5 }, 1, "gcm", new byte[]{ 6, 7, 8 });
        var edek2 = new CipherTrustEdek(KEY_ID, new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5 }, 1, "gcm", new byte[]{ 6, 7, 8 });

        assertThat(edek1).hasSameHashCodeAs(edek2);
    }

    @Test
    void shouldImplementToString() {
        var edek = new CipherTrustEdek(KEY_ID, new byte[]{ 1, 2 }, new byte[]{ 3 }, 1, "gcm", new byte[]{ 4, 5 });
        var toString = edek.toString();

        assertThat(toString)
                .contains(KEY_ID)
                .contains("gcm")
                .contains("version=1");
    }

}
