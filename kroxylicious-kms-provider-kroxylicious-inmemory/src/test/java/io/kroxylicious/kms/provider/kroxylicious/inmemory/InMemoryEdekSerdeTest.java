/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.service.Serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class InMemoryEdekSerdeTest {

    @Test
    void shouldRoundTripAllAllowedAuthBits() {
        for (int bits : new int[]{ 128, 120, 112, 104, 96 }) {
            var edek = new InMemoryEdek(bits, new byte[0], UUID.randomUUID(), new byte[0]);
            Serde<InMemoryEdek> serde = InMemoryEdekSerde.instance();
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
