/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AwsKmsEdekSerdeTest {

    private final AwsKmsEdekSerde serde = new AwsKmsEdekSerde();

    static Stream<Arguments> keyRefs() {
        return Stream.of(
                Arguments.of("keyref", "1234abcd-12ab-34cd-56ef-1234567890ab"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "keyRefs")
    void shouldRoundTrip(String name, String keyRef) {
        var edek = new AwsKmsEdek(keyRef, new byte[]{ 1, 2, 3 });
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        buf.flip();
        var deserialized = serde.deserialize(buf);
        assertThat(deserialized).isEqualTo(edek);
    }

    static Stream<Arguments> sizeOf() {
        return Stream.of(
                Arguments.of(
                        "key id",
                        new AwsKmsEdek("1234abcd-12ab-34cd-56ef-1234567890ab", new byte[]{ 1 }),
                        1 + 1 + 36 + 1));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void sizeOf(String name, AwsKmsEdek edek, int expectedSize) {
        var size = serde.sizeOf(edek);
        assertThat(size).isEqualTo(expectedSize);
    }

    static Stream<Arguments> deserializeErrors() {
        return Stream.of(
                Arguments.of("wrong version", new byte[]{ 1 }),
                Arguments.of("nokek", new byte[]{ 0, 0 }),
                Arguments.of("noekekbytes", new byte[]{ 0, 3, 'A', 'B', 'C' }));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void deserializeErrors(String name, byte[] serializedBytes) {
        var buf = ByteBuffer.wrap(serializedBytes);
        assertThatThrownBy(() -> serde.deserialize(buf))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
