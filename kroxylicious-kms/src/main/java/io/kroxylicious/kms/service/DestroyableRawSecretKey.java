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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A SecretKey that has RAW encoding and can be {@linkplain #destroy() destroyed}
 * (unlike {@link javax.crypto.spec.SecretKeySpec}).
 */
@NotThreadSafe
public final class DestroyableRawSecretKey implements SecretKey {

    private static final Logger LOGGER = LoggerFactory.getLogger(DestroyableRawSecretKey.class);

    private final String algorithm;
    private boolean destroyed = false;
    private final byte[] key;

    private DestroyableRawSecretKey(String algorithm, byte[] bytes) {
        this.algorithm = Objects.requireNonNull(algorithm).toLowerCase(Locale.ROOT);
        this.key = Objects.requireNonNull(bytes);
    }

    /**
     * Create a new key by becoming owner of the given key material.
     * The caller should not modify the given bytes after calling this method.
     * @param algorithm The key algorithm
     * @param bytes The key material
     * @return The new key
     */
    public static @NonNull DestroyableRawSecretKey byOwnershipTransfer(@NonNull String algorithm, @NonNull byte[] bytes) {
        return new DestroyableRawSecretKey(algorithm, bytes);
    }

    /**
     * Create a new key by creating a copy of the given key material.
     * @param algorithm The key algorithm
     * @param bytes The key material
     * @return The new key
     */
    public static @NonNull DestroyableRawSecretKey byClone(@NonNull String algorithm, @NonNull byte[] bytes) {
        return new DestroyableRawSecretKey(algorithm, Objects.requireNonNull(bytes).clone());
    }

    /**
     * Convert the given key to a destroyable key, attempting to destroy the given key.
     * @param source The key to convert
     * @return The new destroyable key.
     */
    public static @NonNull DestroyableRawSecretKey toDestroyableKey(@NonNull SecretKey source) {
        Objects.requireNonNull(source);
        if (!"RAW".equalsIgnoreCase(source.getFormat())) {
            throw new IllegalArgumentException("RAW-format key required");
        }
        // no need to copy, because getEncoded should itself return a copy of the key material
        var result = byOwnershipTransfer(source.getAlgorithm(), source.getEncoded());
        try {
            source.destroy();
        }
        catch (DestroyFailedException e) {
            LOGGER.warn("Failed to destroy key of {}", source.getClass());
        }
        return result;
    }

    @Override
    public @NonNull String getAlgorithm() {
        return algorithm;
    }

    @Override
    public @NonNull String getFormat() {
        return "RAW";
    }

    /**
     * Returns the RAW-encoded key.
     * This is a copy the key. It is the callers responsibility to destroy this key material
     * when it's no longer needed.
     * @return The RAW-encoded key.
     * @throws IllegalStateException if the key has been destroyed
     */
    @Override
    public @NonNull byte[] getEncoded() {
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

    /**
     * Tests whether the arguments represent the same key.
     * @param thisKey The one key
     * @param thatKey The other key
     * @return true if they keys have the same algorithm and key material.
     */
    public static boolean same(@NonNull DestroyableRawSecretKey thisKey, @NonNull DestroyableRawSecretKey thatKey) {
        if (thisKey == thatKey) {
            return true;
        }
        Objects.requireNonNull(thisKey).checkNotDestroyed();
        Objects.requireNonNull(thatKey).checkNotDestroyed();
        return thisKey.algorithm.equals(thatKey.algorithm)
                && MessageDigest.isEqual(thisKey.key, thatKey.key); // note: constant time impl
    }
}
