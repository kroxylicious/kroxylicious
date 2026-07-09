/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust;

import java.util.Arrays;
import java.util.Objects;

/**
 * A CipherTrust Manager Encrypted DEK (EDEK).
 * <br/>
 * Contains all data needed to decrypt a DEK using CipherTrust Manager.
 * <br/>
 * NOTE: we don't actually require the version in the EDEK as CipherTrust Manager
 * identifies key versions with a unique KEK identifier.
 *
 * @param id KEK identifier used for encryption
 * @param ciphertext encrypted DEK bytes
 * @param tag authentication tag for GCM mode
 * @param version key version
 * @param mode encryption mode (e.g., "gcm")
 * @param iv initialization vector
 */
public record CipherTrustEdek(
                              String id,
                              @SuppressWarnings("ArrayRecordComponent") byte[] ciphertext,
                              @SuppressWarnings("ArrayRecordComponent") byte[] tag,
                              int version,
                              String mode,
                              @SuppressWarnings("ArrayRecordComponent") byte[] iv) {

    /**
     * Constructs a CipherTrust encrypted data encryption key.
     */
    public CipherTrustEdek {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(ciphertext, "ciphertext cannot be null");
        Objects.requireNonNull(tag, "tag cannot be null");
        Objects.requireNonNull(mode, "mode cannot be null");
        Objects.requireNonNull(iv, "iv cannot be null");

        if (id.isEmpty()) {
            throw new IllegalArgumentException("id cannot be empty");
        }
        if (ciphertext.length == 0) {
            throw new IllegalArgumentException("ciphertext cannot be empty");
        }
        if (tag.length == 0) {
            throw new IllegalArgumentException("tag cannot be empty");
        }
        if (mode.isEmpty()) {
            throw new IllegalArgumentException("mode cannot be empty");
        }
        if (iv.length == 0) {
            throw new IllegalArgumentException("iv cannot be empty");
        }
    }

    /**
     * Overridden to provide deep equality on the {@code byte[]} fields.
     *
     * @param o the reference object with which to compare
     * @return true iff this object is equal to the given object
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CipherTrustEdek that)) {
            return false;
        }
        return version == that.version
                && Objects.equals(id, that.id)
                && Arrays.equals(ciphertext, that.ciphertext)
                && Arrays.equals(tag, that.tag)
                && Objects.equals(mode, that.mode)
                && Arrays.equals(iv, that.iv);
    }

    /**
     * Overridden to provide a deep hashcode on the {@code byte[]} fields.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        int result = Objects.hash(id, version, mode);
        result = 31 * result + Arrays.hashCode(ciphertext);
        result = 31 * result + Arrays.hashCode(tag);
        result = 31 * result + Arrays.hashCode(iv);
        return result;
    }

    /**
     * Overridden to provide a deep {@code toString()} on the {@code byte[]} fields.
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        return "CipherTrustEdek{" +
                "id='" + id + '\'' +
                ", ciphertext=<redacted>" +
                ", tag=<redacted>" +
                ", version=" + version +
                ", mode='" + mode + '\'' +
                ", iv=<redacted>" +
                '}';
    }
}
