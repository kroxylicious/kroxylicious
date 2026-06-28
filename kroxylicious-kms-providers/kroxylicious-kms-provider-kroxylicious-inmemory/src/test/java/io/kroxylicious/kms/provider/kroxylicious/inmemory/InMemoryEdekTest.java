/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class InMemoryEdekTest {

    private static final UUID KEK_REF = UUID.randomUUID();

    @Test
    void testEqualsAndHashCode() {
        var edek1 = new InMemoryEdek(96, new byte[]{ (byte) 1, (byte) 2, (byte) 3 },
                KEK_REF, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });

        var edek2 = new InMemoryEdek(96, new byte[]{ (byte) 1, (byte) 2, (byte) 3 },
                KEK_REF, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });

        var edek3 = new InMemoryEdek(96, new byte[]{ (byte) 4, (byte) 5, (byte) 6 },
                KEK_REF, new byte[]{ (byte) 1, (byte) 2, (byte) 3 });

        assertEquals(edek1, edek1);
        assertEquals(edek1, edek2);
        assertEquals(edek2, edek2);
        assertEquals(edek2, edek1);
        assertNotEquals(edek1, edek3);
        assertNotEquals(edek3, edek1);
        assertNotEquals(edek2, edek3);
        assertNotEquals(edek3, edek2);
        assertNotEquals("bob", edek1);

        assertEquals(edek1.hashCode(), edek2.hashCode());
        assertNotEquals(edek1.hashCode(), edek3.hashCode());
        assertNotEquals(edek2.hashCode(), edek3.hashCode());
    }

    @Test
    void testToString() {
        var edek1 = new InMemoryEdek(96, new byte[]{ (byte) 1, (byte) 2, (byte) 3 },
                KEK_REF, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });
        assertEquals("InMemoryEdek{numAuthBits=96, iv=[1, 2, 3], edek=[4, 5, 6]}", edek1.toString());

    }

}
