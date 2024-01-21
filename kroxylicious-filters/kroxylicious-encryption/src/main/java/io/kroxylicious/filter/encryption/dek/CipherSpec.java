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

import io.kroxylicious.filter.encryption.EncryptionException;

/**
 * A CipherSpec couples a single persisted identifier with a Cipher (e.g. AES)
 * and means of generating, writing and reading parameters for that cipher.
 */
public enum CipherSpec {

    /**
     * AES/GCM with 96 bit IV and 128 bit auth code.
     */
    AES_GCM_96_128(1, "AES/GCM/NoPadding") {
        Supplier<AlgorithmParameterSpec> paramSupplier() {
            var generator = new AesGcmIvGenerator(new SecureRandom());
            var iv = new byte[12];
            return () -> {
                generator.generateIv(iv);
                return new GCMParameterSpec(128, iv);
            };
        }

        @Override
        int size(AlgorithmParameterSpec parameterSpec) {
            return 12;
        }

        @Override
        void writeParameters(
                ByteBuffer parametersBuffer,
                AlgorithmParameterSpec params
        ) {
            parametersBuffer.put(((GCMParameterSpec)params).getIV());
            parametersBuffer.flip();
        }

        @Override
        GCMParameterSpec readParameters(ByteBuffer parametersBuffer) {
            byte[] b = new byte[12];
            parametersBuffer.get(b).rewind();
            return new GCMParameterSpec(128, b);
        }
    },
    CHACHA20_POLY1305(2, "ChaCha20-Poly1305") {
        int nonceSizeBytes = 12;
        @Override
        Supplier<AlgorithmParameterSpec> paramSupplier() {
            byte[] nonce = new byte[nonceSizeBytes];
            var rng = new SecureRandom();
            return () -> {
                rng.nextBytes(nonce);
                return new IvParameterSpec(nonce);
            };
        }

        @Override
        int size(AlgorithmParameterSpec parameterSpec) {
            return nonceSizeBytes;
        }

        @Override
        void writeParameters(
                ByteBuffer parametersBuffer,
                AlgorithmParameterSpec params
        ) {
            parametersBuffer.put(((IvParameterSpec)params).getIV());
            parametersBuffer.flip();
        }

        @Override
        AlgorithmParameterSpec readParameters(ByteBuffer parametersBuffer) {
            byte[] nonce = new byte[nonceSizeBytes];
            parametersBuffer.get(nonce).rewind();
            return new IvParameterSpec(nonce);
        }
    };

    static CipherSpec fromPersistentId(int persistentId) {
        switch (persistentId) {
            case 1:
                return CipherSpec.AES_GCM_96_128;
            case 2:
                return CipherSpec.CHACHA20_POLY1305;
            default:
                throw new UnknownCipherSpecException("Cipher spec with persistent id " + persistentId + " is not known");
        }
    }

    private final int persistentId;
    private final String transformation;

    CipherSpec(int persistentId, String transformation) {
        this.persistentId = persistentId;
        this.transformation = transformation;
    }

    int persistentId() {
        return persistentId;
    }

    Cipher newCipher() {
        try {
            return Cipher.getInstance(transformation);
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new EncryptionException(e);
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
