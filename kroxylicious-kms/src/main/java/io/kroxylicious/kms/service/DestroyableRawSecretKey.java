/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

import javax.annotation.concurrent.NotThreadSafe;
import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A SecretKey that has RAW encoding and can be {@linkplain #destroy() destroyed}
 * (unlike {@link javax.crypto.spec.SecretKeySpec}).
 */
@NotThreadSafe
public final class DestroyableRawSecretKey implements SecretKey {

    private final String algorithm;
    private boolean destroyed = false;
    private final byte[] key;

    public DestroyableRawSecretKey(String algorithm, byte[] bytes) {
        this(algorithm, bytes, true);
    }

    private DestroyableRawSecretKey(String algorithm, byte[] bytes, boolean cloneBytes) {
        this.algorithm = Objects.requireNonNull(algorithm).toLowerCase(Locale.ENGLISH);
        this.key = cloneBytes ? Objects.requireNonNull(bytes).clone() : bytes;
    }

    public static DestroyableRawSecretKey toDestroyableKey(@NonNull SecretKey source) {
        Objects.requireNonNull(source);
        var result = new DestroyableRawSecretKey(source.getAlgorithm(), Objects.requireNonNull(source.getEncoded()), false);
        try {
            source.destroy();
        }
        catch (DestroyFailedException e) {

        }
        return result;
    }

    @Override
    public String getAlgorithm() {
        return algorithm;
    }

    @Override
    public String getFormat() {
        return "RAW";
    }

    /**
     * Returns the RAW-encoded key.
     * This is a copy the key. It is the callers responsibility to destroy this key material
     * when it's no longer needed.
     * @return The RAW-encoded key.
     */
    @Override
    public byte[] getEncoded() {
        checkNotDestroyed();
        return key.clone();
    }

    private void checkNotDestroyed() {
        if (isDestroyed()) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void destroy() {
        Arrays.fill(key, (byte) 0);
        destroyed = true;
    }

    @Override
    public boolean isDestroyed() {
        return destroyed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DestroyableRawSecretKey that = (DestroyableRawSecretKey) o;
        return destroyed == that.destroyed
                && algorithm.equals(that.algorithm)
                && MessageDigest.isEqual(key, that.key); // note: constant time impl
    }

    @Override
    public int hashCode() {
        int retval = 0;
        for (int i = 1; i < this.key.length; i++) {
            retval += this.key[i] * i;
        }
        return retval ^ this.algorithm.hashCode();
    }
}
