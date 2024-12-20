/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.util.Arrays;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Fortanix DSM Encrypted Dek.
 *
 * @param kekRef - kek reference.
 * @param edek - edek bytes
 * @param iv iv bytes
 */
record FortanixDsmKmsEdek(@NonNull String kekRef,
                          @NonNull byte[] edek,
                          @NonNull byte[] iv) {

    public static final int IV_LENGTH = 16;

    FortanixDsmKmsEdek {
        Objects.requireNonNull(kekRef);
        Objects.requireNonNull(edek);
        Objects.requireNonNull(iv);
        if (kekRef.isEmpty()) {
            throw new IllegalArgumentException("keyRef cannot be empty");
        }
        if (edek.length == 0) {
            throw new IllegalArgumentException("edek cannot be empty");
        }
        if (iv.length != IV_LENGTH) {
            throw new IllegalArgumentException("iv must be 16 bytes");
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
        return Objects.equals(kekRef, that.kekRef) && Arrays.equals(edek, that.edek) && Arrays.equals(iv, that.iv);
    }

    /**
     * Overridden to provide a deep hashcode on the {@code byte[]}.
     * @return the has code.
     */
    @Override
    public int hashCode() {
        int result = Objects.hashCode(kekRef);
        result = 31 * result + Arrays.hashCode(edek) + Arrays.hashCode(iv);
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
                ", edek=" + Arrays.toString(edek) +
                ", iv=" + Arrays.toString(iv) +
                '}';
    }
}
