/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class InMemoryEdekSerdeTest {

    @Test
    void shouldRoundTripAllAllowedAuthBits() {
        for (int bits : new int[]{ 128, 120, 112, 104, 96 }) {
            var edek = new InMemoryEdek(bits, new byte[0], new byte[0]);
            InMemoryEdekSerde serde = new InMemoryEdekSerde();
            int size = serde.sizeOf(edek);
            var buffer = ByteBuffer.allocate(size);
            serde.serialize(edek, buffer);
            assertFalse(buffer.hasRemaining());
            buffer.flip();
            var read = serde.deserialize(buffer);
            assertEquals(edek, read);
        }
    }

}
