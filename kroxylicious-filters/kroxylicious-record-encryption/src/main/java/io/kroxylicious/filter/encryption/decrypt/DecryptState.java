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

    public DecryptState(
            Encryption encryptionUsed
    ) {
        this.encryptionUsed = encryptionUsed;
        this.decryptor = null;
    }

    public boolean isNone() {
        return encryptionUsed == null;
    }

    public DecryptState<E> withDecryptor(Dek<E>.Decryptor decryptor) {
        this.decryptor = decryptor;
        return this;
    }

    public Encryption encryptionUsed() {
        return encryptionUsed;
    }

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
        return Objects.equals(this.encryptionUsed, that.encryptionUsed)
               &&
               Objects.equals(this.decryptor, that.decryptor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(encryptionUsed, decryptor);
    }

    @Override
    public String toString() {
        return "DecryptState["
               +
               "decryptionVersion="
               + encryptionUsed
               + ", "
               +
               "decryptor="
               + decryptor
               + ']';
    }

}
