/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.util.Arrays;
import java.util.Objects;

/**
 * A Fortanix DSM Encrypted Dek.
 *
 * @param kekRef - kek reference.
 * @param iv dek iv bytes
 * @param edek - edek bytes
 */
record FortanixDsmKmsEdek(String kekRef,
                          byte[] iv,
                          byte[] edek) {

    public static final int IV_LENGTH = 16;

    FortanixDsmKmsEdek {
        Objects.requireNonNull(kekRef);
        Objects.requireNonNull(iv);
        Objects.requireNonNull(edek);
        if (kekRef.isEmpty()) {
            throw new IllegalArgumentException("keyRef cannot be empty");
        }
        if (iv.length != IV_LENGTH) {
            throw new IllegalArgumentException("iv must be 16 bytes");
        }
        if (edek.length == 0) {
            throw new IllegalArgumentException("edek cannot be empty");
        }
    }

    /**
     * Overridden to provide deep equality on the {@code byte[]}.
     * @param o   the reference object with which to compare.
     * @return true iff this object is equal to the given object.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FortanixDsmKmsEdek that = (FortanixDsmKmsEdek) o;
        return Objects.equals(kekRef, that.kekRef) && Arrays.equals(iv, that.iv) && Arrays.equals(edek, that.edek);
    }

    /**
     * Overridden to provide a deep hashcode on the {@code byte[]}.
     * @return the has code.
     */
    @Override
    public int hashCode() {
        int result = Objects.hashCode(kekRef);
        result = 31 * result + Arrays.hashCode(iv) + Arrays.hashCode(edek);
        return result;
    }

    /**
     * Overridden to provide a deep {@code toString()} on the {@code byte[]}.
     * @return The string
     */
    @Override
    public String toString() {
        return "FortanixDsmKmsEdek{" +
                "keyRef=" + kekRef +
                ", iv=" + Arrays.toString(iv) +
                ", edek=" + Arrays.toString(edek) +
                '}';
    }
}
