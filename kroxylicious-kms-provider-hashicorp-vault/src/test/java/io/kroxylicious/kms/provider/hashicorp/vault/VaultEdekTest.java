/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VaultEdekTest {
    @Test
    void equalsAndHashCode() {
        var edek1 = new VaultEdek("keyref", new byte[]{ (byte) 1, (byte) 2, (byte) 3 });
        var edek2 = new VaultEdek("keyref", new byte[]{ (byte) 1, (byte) 2, (byte) 3 });
        var keyRefDiffer = new VaultEdek("keyrefX", new byte[]{ (byte) 1, (byte) 2, (byte) 3 });
        var edekBytesDiffer = new VaultEdek("keyref", new byte[]{ (byte) 1, (byte) 2, (byte) 4 });

        assertThat(edek1)
                .isEqualTo(edek1)
                .isNotEqualTo(new Object())
                .isNotEqualTo(null);

        assertThat(edek1)
                .isEqualTo(edek2)
                .hasSameHashCodeAs(edek2);
        assertThat(edek2).isEqualTo(edek1);

        assertThat(edek1)
                .isNotEqualTo(keyRefDiffer)
                .doesNotHaveSameHashCodeAs(keyRefDiffer);

        assertThat(edek1)
                .isNotEqualTo(edekBytesDiffer)
                .doesNotHaveSameHashCodeAs(edekBytesDiffer);
    }

    @Test
    void toStringFormation() {
        var edek = new VaultEdek("keyref", new byte[]{ (byte) 1, (byte) 2, (byte) 3 });
        assertThat(edek).hasToString("VaultEdek{keyRef=keyref, edek=[1, 2, 3]}");
    }
}
