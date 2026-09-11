/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryEdekTest {

    private static final UUID KEK_REF = UUID.randomUUID();

    @Test
    void testEqualsAndHashCode() {
        var edek1 = new InMemoryEdek(96, new byte[]{ (byte) 1, (byte) 2, (byte) 3 },
                KEK_REF, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });
        var edek2 = new InMemoryEdek(96, new byte[]{ (byte) 1, (byte) 2, (byte) 3 },
                KEK_REF, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });
        var numAuthBitsDiffer = new InMemoryEdek(128, new byte[]{ (byte) 1, (byte) 2, (byte) 3 },
                KEK_REF, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });
        var ivDiffer = new InMemoryEdek(96, new byte[]{ (byte) 7, (byte) 8, (byte) 9 },
                KEK_REF, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });
        var edekBytesDiffer = new InMemoryEdek(96, new byte[]{ (byte) 1, (byte) 2, (byte) 3 },
                KEK_REF, new byte[]{ (byte) 7, (byte) 8, (byte) 9 });

        assertThat(edek1)
                .isNotEqualTo(new Object())
                .isNotEqualTo(null);

        assertThat(edek1)
                .isEqualTo(edek2)
                .hasSameHashCodeAs(edek2);
        assertThat(edek2).isEqualTo(edek1);

        assertThat(edek1)
                .isNotEqualTo(numAuthBitsDiffer)
                .doesNotHaveSameHashCodeAs(numAuthBitsDiffer);

        assertThat(edek1)
                .isNotEqualTo(ivDiffer)
                .doesNotHaveSameHashCodeAs(ivDiffer);

        assertThat(edek1)
                .isNotEqualTo(edekBytesDiffer)
                .doesNotHaveSameHashCodeAs(edekBytesDiffer);
    }

    @Test
    void testToString() {
        var edek1 = new InMemoryEdek(96, new byte[]{ (byte) 1, (byte) 2, (byte) 3 },
                KEK_REF, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });
        assertThat(edek1).hasToString("InMemoryEdek{numAuthBits=96, iv=[1, 2, 3], edek=[4, 5, 6]}");

    }

}
