/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kms.service.Serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AwsKmsEdekSerdeTest {

    private static final String KEY_REF = "1234abcd-12ab-34cd-56ef-1234567890ab";
    private final Serde<AwsKmsEdek> serde = AwsKmsEdekSerde.instance();

    @Test
    void shouldRoundTrip() {
        var edek = new AwsKmsEdek(KEY_REF, new byte[]{ 1, 2, 3 });
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        buf.flip();
        var deserialized = serde.deserialize(buf);
        assertThat(deserialized).isEqualTo(edek);
    }

    @Test
    void sizeOf() {
        var edek = new AwsKmsEdek(KEY_REF, new byte[]{ 1 });
        var expectedSize = 1 + 1 + 36 + 1;
        var size = serde.sizeOf(edek);
        assertThat(size).isEqualTo(expectedSize);
    }

    static Stream<Arguments> deserializeErrors() {
        return Stream.of(
                Arguments.of("wrong version", new byte[]{ 1 }),
                Arguments.of("nokek", new byte[]{ 0, 0 }),
                Arguments.of("noekekbytes", new byte[]{ 0, 3, 'A', 'B', 'C' })
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void deserializeErrors(String name, byte[] serializedBytes) {
        var buf = ByteBuffer.wrap(serializedBytes);
        assertThatThrownBy(() -> serde.deserialize(buf))
                                                        .isInstanceOf(IllegalArgumentException.class);
    }

}
