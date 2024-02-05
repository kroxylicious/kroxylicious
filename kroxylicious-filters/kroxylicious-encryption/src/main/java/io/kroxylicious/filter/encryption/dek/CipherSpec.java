/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.function.Supplier;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;

/**
 * A CipherSpec couples a single persisted identifier with a Cipher (e.g. AES)
 * and means of generating, writing and reading parameters for that cipher.
 */
public enum CipherSpec {

    /**
     * AES/GCM with 128-bit key, 96-bit IV and 128-bit tag.
     * @see <a href="https://www.ietf.org/rfc/rfc5116.txt">RFC-5116</a>
     */
    AES_128_GCM_128(0,
            "AES/GCM/NoPadding",
            1L << 32 // 2^32
    ) {

        private static final int IV_SIZE_BYTES = 12;
        private static final int TAG_LENGTH_BITS = 128;

        Supplier<AlgorithmParameterSpec> paramSupplier() {
            var generator = new Wrapping96BitCounter(new SecureRandom());
            var iv = new byte[IV_SIZE_BYTES];
            return () -> {
                generator.generateIv(iv);
                return new GCMParameterSpec(TAG_LENGTH_BITS, iv);
            };
        }

        @Override
        int size(AlgorithmParameterSpec parameterSpec) {
            return IV_SIZE_BYTES;
        }

        @Override
        void writeParameters(
                             ByteBuffer parametersBuffer,
                             AlgorithmParameterSpec params) {
            parametersBuffer.put(((GCMParameterSpec) params).getIV());
        }

        @Override
        GCMParameterSpec readParameters(ByteBuffer parametersBuffer) {
            byte[] b = new byte[IV_SIZE_BYTES];
            parametersBuffer.get(b);
            return new GCMParameterSpec(TAG_LENGTH_BITS, b);
        }
    },
    /**
     * ChaCha20-Poly1305, which means 256-bit key, 96-bit nonce and 128-bit tag.
     * @see <a href="https://www.ietf.org/rfc/rfc7539.txt">RFC-7539</a>
     */
    CHACHA20_POLY1305(1,
            "ChaCha20-Poly1305",
            Long.MAX_VALUE // 2^96 would be necessary given we use Wrapping96BitCounter
    // 2^63-1 is sufficient
    ) {
        private static final int NONCE_SIZE_BYTES = 12;

        @Override
        Supplier<AlgorithmParameterSpec> paramSupplier() {
            // Per https://www.rfc-editor.org/rfc/rfc7539#section-4
            // we generate the nonce using a counter
            var generator = new Wrapping96BitCounter(new SecureRandom());
            var nonce = new byte[NONCE_SIZE_BYTES];
            return () -> {
                generator.generateIv(nonce);
                return new IvParameterSpec(nonce);
            };
        }

        @Override
        int size(AlgorithmParameterSpec parameterSpec) {
            return NONCE_SIZE_BYTES;
        }

        @Override
        void writeParameters(
                             ByteBuffer parametersBuffer,
                             AlgorithmParameterSpec params) {
            parametersBuffer.put(((IvParameterSpec) params).getIV());
        }

        @Override
        AlgorithmParameterSpec readParameters(ByteBuffer parametersBuffer) {
            byte[] nonce = new byte[NONCE_SIZE_BYTES];
            parametersBuffer.get(nonce);
            return new IvParameterSpec(nonce);
        }
    };

    static CipherSpec fromPersistentId(int persistentId) {
        switch (persistentId) {
            case 0:
                return CipherSpec.AES_128_GCM_128;
            case 1:
                return CipherSpec.CHACHA20_POLY1305;
            default:
                throw new UnknownCipherSpecException("Cipher spec with persistent id " + persistentId + " is not known");
        }
    }

    private final int persistentId;
    private final String transformation;

    private final long maxEncryptionsPerKey;

    CipherSpec(int persistentId, String transformation, long maxEncryptionsPerKey) {
        this.persistentId = persistentId;
        this.transformation = transformation;
        this.maxEncryptionsPerKey = maxEncryptionsPerKey;
    }

    public int persistentId() {
        return persistentId;
    }

    public long maxEncryptionsPerKey() {
        return maxEncryptionsPerKey;
    }

    Cipher newCipher() {
        try {
            return Cipher.getInstance(transformation);
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new DekException(e);
        }
    }

    /**
     * Return a supplier of parameters for use with the cipher.
     * The supplier need not be thread-safe.
     */
    abstract Supplier<AlgorithmParameterSpec> paramSupplier();

    /**
     * Return the number of bytes required by {@link #writeParameters(ByteBuffer, AlgorithmParameterSpec)}
     * to serialize the given parameters.
     */
    abstract int size(AlgorithmParameterSpec parameterSpec);

    /**
     * Serialize the given parameters to the given buffer, which should have at least
     * {@link #size(AlgorithmParameterSpec)} bytes {@linkplain ByteBuffer#remaining() remaining}.
     */
    abstract void writeParameters(ByteBuffer parametersBuffer, AlgorithmParameterSpec params);

    /**
     * Read previously-serialize parameters from the given buffer.
     * The implementation should know how many bytes to read, so the number of
     * {@linkplain ByteBuffer#remaining() remaining} bytes need only be ≥ (not =)
     * the {@link #size(AlgorithmParameterSpec)} at the time the buffer was written.
     */
    abstract AlgorithmParameterSpec readParameters(ByteBuffer parametersBuffer);
}
