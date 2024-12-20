/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class FortanixDsmKmsEdekTest {

    public static Stream<Arguments> illegalDeks() {
        return Stream.of(
                argumentSet("empty kek", (Supplier<FortanixDsmKmsEdek>) () -> new FortanixDsmKmsEdek("", new byte[]{ 1 }, new byte[]{ 2 })),
                argumentSet("empty bytes", (Supplier<FortanixDsmKmsEdek>) () -> new FortanixDsmKmsEdek("k", new byte[]{}, new byte[]{ 2 })),
                argumentSet("empty iv", (Supplier<FortanixDsmKmsEdek>) () -> new FortanixDsmKmsEdek("", new byte[]{ 1 }, new byte[]{})));
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
        var edek1 = new FortanixDsmKmsEdek("keyref", new byte[]{ (byte) 1, (byte) 2, (byte) 3 }, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });
        var edek2 = new FortanixDsmKmsEdek("keyref", new byte[]{ (byte) 1, (byte) 2, (byte) 3 }, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });
        var keyRefDiffer = new FortanixDsmKmsEdek("keyrefX", new byte[]{ (byte) 1, (byte) 2, (byte) 3 }, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });
        var edekBytesDiffer = new FortanixDsmKmsEdek("keyref", new byte[]{ (byte) 1, (byte) 2, (byte) 4 }, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });
        var edekIvBytesDiffer = new FortanixDsmKmsEdek("keyref", new byte[]{ (byte) 1, (byte) 2, (byte) 3 }, new byte[]{ (byte) 4, (byte) 5, (byte) 7 });

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
        var edek = new FortanixDsmKmsEdek("keyref", new byte[]{ (byte) 1, (byte) 2, (byte) 3 }, new byte[]{ (byte) 4, (byte) 5, (byte) 6 });
        assertThat(edek).hasToString("FortanixDsmKmsEdek{keyRef=keyref, edek=[1, 2, 3], iv=[4, 5, 6]}");
    }
}
