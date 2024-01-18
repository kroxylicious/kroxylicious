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
            parametersBuffer.get(b);
            return new GCMParameterSpec(128, b);
        }
    };

    // TODO Add support for ChaCha20Poly1305

    private final int persistentId;
    private final String transformation;

    CipherSpec(int persistentId, String transformation) {
        this.persistentId = persistentId;
        this.transformation = transformation;

    }

    static CipherSpec fromPersistentId(int persistentId) {
        switch (persistentId) {
            case 1:
                return CipherSpec.AES_GCM_96_128;
            default:
                throw new UnknownCipherSpecException("Cipher spec with persistent id " + persistentId + " is not known");
        }
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

    abstract Supplier<AlgorithmParameterSpec> paramSupplier();

    abstract int size(AlgorithmParameterSpec parameterSpec);

    abstract void writeParameters(ByteBuffer parametersBuffer, AlgorithmParameterSpec params);

    abstract AlgorithmParameterSpec readParameters(ByteBuffer parametersBuffer);
}
