/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kms.service.Serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FortanixDsmKmsEdekSerdeTest {

    private static final String KEY_REF = "1234abcd-12ab-34cd-56ef-1234567890ab";
    private static final byte[] EDEK = { 1, 2, 3 };
    static final byte[] IV = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    static final byte[] ANOTHER_IV = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0 };
    private final Serde<FortanixDsmKmsEdek> serde = FortanixDsmKmsEdekSerde.instance();

    @Test
    void shouldRoundTrip() {
        var edek = new FortanixDsmKmsEdek(KEY_REF, IV, EDEK);
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        buf.flip();
        var deserialized = serde.deserialize(buf);
        assertThat(deserialized).isEqualTo(edek);
    }

    @Test
    void sizeOf() {
        var edek = new FortanixDsmKmsEdek(KEY_REF, IV, new byte[]{ 1 });
        var expectedSize = 1 + 1 + 36 + 16 + 1;
        var size = serde.sizeOf(edek);
        assertThat(size).isEqualTo(expectedSize);
    }

    static Stream<Arguments> deserializeErrors() {
        return Stream.of(
                Arguments.argumentSet("wrong version", new byte[]{ 1 }),
                Arguments.argumentSet("nokek", new byte[]{ 0, 0 }),
                Arguments.argumentSet("noivbytes", new byte[]{ 0, 3, 'A', 'B', 'C' }),
                Arguments.argumentSet("noivbytes", new byte[]{ 0, 3, 'A', 'B', 'C', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }));
    }

    @ParameterizedTest
    @MethodSource
    void deserializeErrors(byte[] serializedBytes) {
        var buf = ByteBuffer.wrap(serializedBytes);
        assertThatThrownBy(() -> serde.deserialize(buf))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deserialiseV0() {
        var v0 = new byte[]{
                0, // version
                36, // kek length
                49, 50, 51, 52, 97, 98, 99, 100, 45, 49, 50, 97, 98, 45, 51, 52, 99, 100, 45, 53, 54, 101, 102, 45, 49, 50, 51, 52, 53, 54, 55, 56, 57, 48, 97, 98, // kek
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                1, 2, 3 };

        var deserialized = serde.deserialize(ByteBuffer.wrap(v0));
        assertThat(deserialized).isEqualTo(new FortanixDsmKmsEdek(KEY_REF, IV, EDEK));
    }

    @Test
    void serialisesAsV0() {
        var edek = new FortanixDsmKmsEdek(KEY_REF, IV, EDEK);
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        buf.flip();
        var version = buf.get();
        assertThat(version).isZero();
    }

}
