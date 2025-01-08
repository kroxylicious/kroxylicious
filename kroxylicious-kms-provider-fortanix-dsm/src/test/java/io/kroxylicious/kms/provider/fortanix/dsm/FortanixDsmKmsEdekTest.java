/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static io.kroxylicious.kms.provider.fortanix.dsm.FortanixDsmKmsEdekSerdeTest.ANOTHER_IV;
import static io.kroxylicious.kms.provider.fortanix.dsm.FortanixDsmKmsEdekSerdeTest.IV;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class FortanixDsmKmsEdekTest {

    public static Stream<Arguments> illegalDeks() {
        return Stream.of(
                argumentSet("empty kek", (Supplier<FortanixDsmKmsEdek>) () -> new FortanixDsmKmsEdek("", IV, new byte[]{ 1 })),
                argumentSet("empty bytes", (Supplier<FortanixDsmKmsEdek>) () -> new FortanixDsmKmsEdek("k", IV, new byte[]{})),
                argumentSet("empty iv", (Supplier<FortanixDsmKmsEdek>) () -> new FortanixDsmKmsEdek("", new byte[]{}, new byte[]{ 1 })));
    }

    @ParameterizedTest
    @MethodSource("illegalDeks")
    void illegalEdeks(Supplier<FortanixDsmKmsEdek> supplier) {
        assertThatThrownBy(supplier::get)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @SuppressWarnings("java:S5853")
    void equalsAndHashCode() {

        var edek1 = new FortanixDsmKmsEdek("keyref", IV, new byte[]{ (byte) 1, (byte) 2, (byte) 3 });
        var edek2 = new FortanixDsmKmsEdek("keyref", IV, new byte[]{ (byte) 1, (byte) 2, (byte) 3 });
        var keyRefDiffer = new FortanixDsmKmsEdek("keyrefX", IV, new byte[]{ (byte) 1, (byte) 2, (byte) 3 });
        var edekBytesDiffer = new FortanixDsmKmsEdek("keyref", IV, new byte[]{ (byte) 1, (byte) 2, (byte) 4 });
        var edekIvBytesDiffer = new FortanixDsmKmsEdek("keyref", ANOTHER_IV, new byte[]{ (byte) 1, (byte) 2, (byte) 3 });

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

        assertThat(edek1)
                .isNotEqualTo(edekIvBytesDiffer)
                .doesNotHaveSameHashCodeAs(edekIvBytesDiffer);
    }

    @Test
    void toStringFormation() {
        var edek = new FortanixDsmKmsEdek("keyref", IV, new byte[]{ (byte) 1, (byte) 2, (byte) 3 });
        assertThat(edek).hasToString("FortanixDsmKmsEdek{keyRef=keyref, iv=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], edek=[1, 2, 3]}");
    }
}
