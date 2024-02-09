/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.NotThreadSafe;
import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.encryption.dek.ExhaustedDekException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Context encapsulates an encryptor for a DEK.
 * <p>
 * KeyContext is not threadsafe and access should be externally co-ordinated.
 * </p>
 */
@NotThreadSafe
final class KeyContext implements Destroyable {
    private final AesGcmEncryptor encryptor;
    private final byte[] serializedEdek;
    private final long encryptionExpiryNanos;
    private int remainingEncryptions;
    private boolean destroyed = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyContext.class);
    private static final Map<Class<? extends Destroyable>, Boolean> LOGGED_DESTROY_FAILED = new ConcurrentHashMap<>();

    KeyContext(@NonNull ByteBuffer serializedEdek,
               long encryptionExpiryNanos,
               int maxEncryptions,
               @NonNull AesGcmEncryptor encryptor) {
        if (maxEncryptions <= 0) {
            throw new IllegalArgumentException();
        }
        this.serializedEdek = serializedEdek.array();
        this.encryptionExpiryNanos = encryptionExpiryNanos;
        this.remainingEncryptions = maxEncryptions;
        this.encryptor = encryptor;
    }

    public byte[] serializedEdek() {
        return serializedEdek;
    }

    public boolean isExpiredForEncryption(long nanoTime) {
        return nanoTime > encryptionExpiryNanos;
    }

    public boolean hasAtLeastRemainingEncryptions(int numEncryptions) {
        if (numEncryptions <= 0) {
            throw new IllegalArgumentException();
        }
        return remainingEncryptions >= numEncryptions;
    }

    public void recordEncryptions(int numEncryptions) {
        if (numEncryptions < 0) {
            throw new IllegalArgumentException();
        }
        remainingEncryptions -= numEncryptions;
    }

    /**
     * Returns the size of the encoding of a plaintext of the given size
     * @param plaintextSize The plaintext.
     * @return The size, in bytes, of a plaintext.
     */
    public int encodedSize(int plaintextSize) {
        return encryptor.outputSize(plaintextSize);
    }

    /**
     * Encode the key metadata and the ciphertext of the given {@code plaintext} to the given {@code output},
     * which should have at least {@link #encodedSize(int) encodedSize(plaintext)} bytes {@linkplain ByteBuffer#remaining() remaining}.
     * @param plaintext The plaintext
     * @param output The output buffer
     */
    public void encode(@NonNull ByteBuffer plaintext, @NonNull ByteBuffer output) {
        if (remainingEncryptions <= 0) {
            throw new ExhaustedDekException("No more encryptions");
        }
        encryptor.encrypt(plaintext, output);
    }

    @Override
    public void destroy() {
        destroy(encryptor);
        destroyed = true;
    }

    @Override
    public boolean isDestroyed() {
        return destroyed;
    }

    static void destroy(Destroyable destroyable) {
        try {
            destroyable.destroy();
        }
        catch (DestroyFailedException e) {
            var cls = destroyable.getClass();
            LOGGED_DESTROY_FAILED.computeIfAbsent(cls, (c) -> {
                LOGGER.warn("Failed to destroy an instance of {}. "
                        + "Note: this message is logged once per class even though there may be many occurrences of this event. "
                        + "This event can happen because the JRE's SecretKeySpec class does not override destroy().",
                        c, e);
                return Boolean.TRUE;
            });
        }
    }
}
