/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.decrypt;

import java.util.Objects;

import io.kroxylicious.filter.encryption.config.EncryptionVersion;
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

    private final EncryptionVersion decryptionVersion;
    @Nullable
    private Dek<E>.Decryptor decryptor;

    public DecryptState(
                        EncryptionVersion decryptionVersion) {
        this.decryptionVersion = decryptionVersion;
        this.decryptor = null;
    }

    public boolean isNone() {
        return decryptionVersion == null;
    }

    public DecryptState<E> withDecryptor(Dek<E>.Decryptor decryptor) {
        this.decryptor = decryptor;
        return this;
    }

    public EncryptionVersion decryptionVersion() {
        return decryptionVersion;
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
        return Objects.equals(this.decryptionVersion, that.decryptionVersion) &&
                Objects.equals(this.decryptor, that.decryptor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(decryptionVersion, decryptor);
    }

    @Override
    public String toString() {
        return "DecryptState[" +
                "decryptionVersion=" + decryptionVersion + ", " +
                "decryptor=" + decryptor + ']';
    }

}
