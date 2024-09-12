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

import io.kroxylicious.filter.encryption.config.CipherSpec;

public class Aes implements CipherManager {

    public static final Aes AES_256_GCM_128 = new Aes("AES/GCM/NoPadding", 256, (byte) 0, CipherSpec.AES_256_GCM_128);

    private static final int IV_SIZE_BYTES = 12;
    private static final int TAG_LENGTH_BITS = 128;
    private final String transformation;
    private final byte serializedId;
    private final CipherSpec spec;
    private final SecureRandom rng;
    private final int numKeyBits;

    private Aes(String transformation, int numKeyBits, byte serializedId, CipherSpec spec) {
        this.transformation = transformation;
        if (numKeyBits < 0) {
            throw new IllegalArgumentException();
        }
        this.numKeyBits = numKeyBits;
        this.serializedId = serializedId;
        this.spec = spec;
        rng = new SecureRandom();
    }

    @Override
    public int requiredNumKeyBits() {
        return numKeyBits;
    }

    @Override
    public byte serializedId() {
        return serializedId;
    }

    @Override
    public CipherSpec name() {
        return spec;
    }

    @Override
    public Cipher newCipher() {
        try {
            return Cipher.getInstance(transformation);
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new DekException(e);
        }
    }

    @Override
    public long maxEncryptionsPerKey() {
        return 1L << 32; // 2^32
    }

    @Override
    public Supplier<AlgorithmParameterSpec> paramSupplier() {
        var generator = new Wrapping96BitCounter(rng);
        var iv = new byte[IV_SIZE_BYTES];
        return () -> {
            generator.generateIv(iv);
            return new GCMParameterSpec(TAG_LENGTH_BITS, iv);
        };
    }

    @Override
    public int constantParamsSize() {
        return IV_SIZE_BYTES;
    }

    @Override
    public int size(AlgorithmParameterSpec parameterSpec) {
        return constantParamsSize();
    }

    @Override
    public void writeParameters(
            ByteBuffer parametersBuffer,
            AlgorithmParameterSpec params
    ) {
        parametersBuffer.put(((GCMParameterSpec) params).getIV());
    }

    @Override
    public GCMParameterSpec readParameters(ByteBuffer parametersBuffer) {
        byte[] b = new byte[IV_SIZE_BYTES];
        parametersBuffer.get(b);
        return new GCMParameterSpec(TAG_LENGTH_BITS, b);
    }

}
