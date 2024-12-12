/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.util.Arrays;
import java.util.Objects;

/**
 * An AWS KMS Encrypted Dek.
 *
 * @param kekRef - kek reference.
 * @param edek - edek bytes
 */
record AwsKmsEdek(String kekRef,
                  byte[] edek) {
    AwsKmsEdek {
        Objects.requireNonNull(kekRef);
        Objects.requireNonNull(edek);
        if (kekRef.isEmpty()) {
            throw new IllegalArgumentException("keyRef cannot be empty");
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
        AwsKmsEdek that = (AwsKmsEdek) o;
        return Objects.equals(kekRef, that.kekRef) && Arrays.equals(edek, that.edek);
    }

    /**
     * Overridden to provide a deep hashcode on the {@code byte[]}.
     * @return the has code.
     */
    @Override
    public int hashCode() {
        int result = Objects.hashCode(kekRef);
        result = 31 * result + Arrays.hashCode(edek);
        return result;
    }

    /**
     * Overridden to provide a deep {@code toString()} on the {@code byte[]}.
     * @return The string
     */
    @Override
    public String toString() {
        return "AwsKmsEdek{" +
                "keyRef=" + kekRef +
                ", edek=" + Arrays.toString(edek) +
                '}';
    }
}
