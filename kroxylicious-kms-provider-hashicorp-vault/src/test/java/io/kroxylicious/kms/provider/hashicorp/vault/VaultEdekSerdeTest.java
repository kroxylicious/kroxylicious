/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VaultEdekSerdeTest {

    private final VaultEdekSerde serde = new VaultEdekSerde();

    @Test
    void shouldRoundTrip() {
        var edek = new VaultEdek("keyref", new byte[]{ 1, 2, 3 });
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        buf.flip();
        var deserialized = serde.deserialize(buf);
        assertThat(deserialized).isEqualTo(edek);
    }

    public static Stream<Arguments> serializedBadKek() {
        return Stream.of(
                Arguments.of("empty", new VaultEdek("", new byte[]{ 1, 2, 3 })),
                Arguments.of("toobig", new VaultEdek("x".repeat(256), new byte[]{ 1, 2, 3 })));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void serializedBadKek(String name, VaultEdek edek) {
        var buf = ByteBuffer.allocate(1000);
        assertThatThrownBy(() -> serde.serialize(edek, buf))
                .isInstanceOf(IllegalArgumentException.class);
    }

    public static Stream<Arguments> deserializeErrors() {
        return Stream.of(
                Arguments.of("emptykek", new byte[]{ 0 }),
                Arguments.of("noekekbytes", new byte[]{ 3, 'A', 'B', 'C' }));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void deserializeErrors(String name, byte[] serializedBytes) {
        var buf = ByteBuffer.wrap(serializedBytes);
        assertThatThrownBy(() -> serde.deserialize(buf))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
