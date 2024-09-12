/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

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

    private DestroyableRawSecretKey(byte[] bytes, String algorithm) {
        this.algorithm = Objects.requireNonNull(algorithm).toLowerCase(Locale.ROOT);
        this.key = Objects.requireNonNull(bytes);
    }

    /**
     * @return The number of bits in this key
     */
    public int numKeyBits() {
        return key.length * Byte.SIZE;
    }

    /**
     * Create a new key by becoming owner of the given key material.
     * The caller should not modify the given bytes after calling this method.
     *
     * @param bytes The key material
     * @param algorithm The key algorithm
     * @return The new key
     */
    public static @NonNull DestroyableRawSecretKey takeOwnershipOf(
            @NonNull
            byte[] bytes,
            @NonNull
            String algorithm
    ) {
        return new DestroyableRawSecretKey(bytes, algorithm);
    }

    /**
     * Create a new key by creating a copy of the given key material.
     *
     * @param bytes The key material
     * @param algorithm The key algorithm
     * @return The new key
     */
    public static @NonNull DestroyableRawSecretKey takeCopyOf(
            @NonNull
            byte[] bytes,
            @NonNull
            String algorithm
    ) {
        return new DestroyableRawSecretKey(Objects.requireNonNull(bytes).clone(), algorithm);
    }

    /**
     * Convert the given key to a destroyable key, attempting to destroy the given key.
     * @param source The key to convert
     * @return The new destroyable key.
     */
    public static @NonNull DestroyableRawSecretKey toDestroyableKey(
            @NonNull
            SecretKey source
    ) {
        checkNotDestroyed(Objects.requireNonNull(source));
        if (source instanceof DestroyableRawSecretKey destroyableRawSecretKey) {
            return destroyableRawSecretKey;
        }
        if (!"RAW".equalsIgnoreCase(source.getFormat())) {
            throw new IllegalArgumentException("RAW-format key required");
        }
        // no need to copy, because getEncoded should itself return a copy of the key material
        var result = takeOwnershipOf(source.getEncoded(), source.getAlgorithm());
        try {
            source.destroy();
        }
        catch (DestroyFailedException e) {
            LOGGER.warn("Failed to destroy key of {}: {}", source.getClass(), e);
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
     * This is a copy the key. It is the caller's responsibility to destroy this key material
     * when it's no longer needed.
     * @return The RAW-encoded key.
     * @throws IllegalStateException if the key has been destroyed
     */
    @Override
    public @NonNull byte[] getEncoded() {
        checkNotDestroyed(this);
        return key.clone();
    }

    static void checkNotDestroyed(SecretKey key) {
        if (key.isDestroyed()) {
            throw new IllegalStateException("Key has been destroyed");
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

}
