/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.decrypt;

import java.util.Objects;

import io.kroxylicious.filter.encryption.crypto.Encryption;
import io.kroxylicious.filter.encryption.dek.Dek;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Helper class to group together some state for decryption.
 * Either both, or neither, of the given {@code decryptionVersion} and {@code encryptor} should be null.
 * @param <E> The type of encrypted DEK.
 */
public final class DecryptState<E> {

    @SuppressWarnings("rawtypes")
    private static final DecryptState NONE = new DecryptState(null);

    @SuppressWarnings("unchecked")
    static <E> DecryptState<E> none() {
        return NONE;
    }

    private final Encryption encryptionUsed;
    @Nullable
    private Dek<E>.Decryptor decryptor;

    /**
     * Creates a decrypt state without a decryptor.
     * @param encryptionUsed the encryption used by the record being decrypted, or null if the record was not encrypted.
     */
    public DecryptState(
                        Encryption encryptionUsed) {
        this.encryptionUsed = encryptionUsed;
        this.decryptor = null;
    }

    /**
     * Returns whether this state represents a record which was not encrypted.
     * @return true if the record was not encrypted.
     */
    public boolean isNone() {
        return encryptionUsed == null;
    }

    /**
     * Sets the decryptor to be used to decrypt the record.
     * @param decryptor the decryptor to be used to decrypt the record.
     * @return this decrypt state.
     */
    public DecryptState<E> withDecryptor(Dek<E>.Decryptor decryptor) {
        this.decryptor = decryptor;
        return this;
    }

    /**
     * Returns the encryption used by the record being decrypted, or null if the record was not encrypted.
     * @return the encryption used by the record being decrypted, or null if the record was not encrypted.
     */
    public Encryption encryptionUsed() {
        return encryptionUsed;
    }

    /**
     * Returns the decryptor to be used to decrypt the record, or null if not yet set.
     * @return the decryptor to be used to decrypt the record, or null if not yet set.
     */
    @Nullable
    public Dek<E>.Decryptor decryptor() {
        return decryptor;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (DecryptState) obj;
        return Objects.equals(this.encryptionUsed, that.encryptionUsed) &&
                Objects.equals(this.decryptor, that.decryptor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(encryptionUsed, decryptor);
    }

    @Override
    public String toString() {
        return "DecryptState[" +
                "decryptionVersion=" + encryptionUsed + ", " +
                "decryptor=" + decryptor + ']';
    }

}
