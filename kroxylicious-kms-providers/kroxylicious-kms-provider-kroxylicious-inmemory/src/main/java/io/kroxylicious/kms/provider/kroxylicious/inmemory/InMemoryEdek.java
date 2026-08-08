/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * An encrypted Data Encryption Key (DEK), as produced by {@link InMemoryKms}, wrapped using
 * AES-GCM with a Key Encryption Key (KEK) held in the KMS.
 *
 * @param numAuthBits the length of the GCM authentication tag, in bits.
 * @param iv the initialization vector used when wrapping the DEK.
 * @param kekRef the id of the KEK used to wrap the DEK.
 * @param edek the wrapped DEK.
 */
public record InMemoryEdek(
                           int numAuthBits,
                           @SuppressWarnings("ArrayRecordComponent") byte[] iv, // byte[] retained: deep equality via explicit equals/hashCode below; treated as immutable by convention
                           UUID kekRef,
                           @SuppressWarnings("ArrayRecordComponent") byte[] edek) { // byte[] retained: deep equality via explicit equals/hashCode below; treated as immutable by convention

    /**
     * Validates the record components.
     * @throws IllegalArgumentException if {@code numAuthBits} is not an authentication tag
     * length permitted by NIST.SP.800-38D §5.2.1.2.
     */
    public InMemoryEdek {
        if (numAuthBits != 128
                && numAuthBits != 120
                && numAuthBits != 112
                && numAuthBits != 104
                && numAuthBits != 96) {
            // Per NIST.SP.800-138D §5.2.1.2
            throw new IllegalArgumentException("numAuthBits must be one of 128, 120, 112, 104, or 96");
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
        if (!(o instanceof InMemoryEdek that)) {
            return false;
        }
        return numAuthBits == that.numAuthBits && Arrays.equals(iv, that.iv) && Arrays.equals(edek, that.edek);
    }

    /**
     * Overridden to provide a deep hashcode on the {@code byte[]}.
     * @return the has code.
     */
    @Override
    public int hashCode() {
        int result = Objects.hash(numAuthBits);
        result = 31 * result + Arrays.hashCode(iv);
        result = 31 * result + Arrays.hashCode(edek);
        return result;
    }

    /**
     * Overridden to provide a deep {@code toString()} on the {@code byte[]}.
     * @return The string
     */
    @Override
    public String toString() {
        return "InMemoryEdek{" +
                "numAuthBits=" + numAuthBits +
                ", iv=" + Arrays.toString(iv) +
                ", edek=" + Arrays.toString(edek) +
                '}';
    }
}
