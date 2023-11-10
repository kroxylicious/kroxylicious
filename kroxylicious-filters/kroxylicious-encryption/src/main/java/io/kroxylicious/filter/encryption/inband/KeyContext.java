/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A DekContext encapsulates an encryptor.
 */
final class KeyContext implements Destroyable {
    private final AesGcmEncryptor encryptor;
    private final byte[] prefix;
    private final long encryptionExpiryNanos;
    private final AtomicInteger remainingEncryptions;

    KeyContext(@NonNull ByteBuffer prefix,
               long encryptionExpiryNanos,
               int maxEncryptions,
               @NonNull AesGcmEncryptor encryptor) {
        if (maxEncryptions <= 0) {
            throw new IllegalArgumentException();
        }
        this.prefix = prefix.array();
        this.encryptionExpiryNanos = encryptionExpiryNanos;
        this.remainingEncryptions = new AtomicInteger(maxEncryptions);
        this.encryptor = encryptor;
    }

    public byte[] prefix() {
        return prefix;
    }

    public boolean isExpiredForEncryption(long nanoTime) {
        return nanoTime > encryptionExpiryNanos;
    }

    public boolean hasAtLeastRemainingEncryptions(int numEncryptions) {
        if (numEncryptions <= 0) {
            throw new IllegalArgumentException();
        }
        return remainingEncryptions.getAndAdd(-numEncryptions) >= numEncryptions;
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
        if (remainingEncryptions.get() < 0) {
            throw new ExhaustedDekException("No more encryptions");
        }
        encryptor.encrypt(plaintext, output);
    }

    @Override
    public void destroy() throws DestroyFailedException {
        encryptor.destroy();
    }
}
