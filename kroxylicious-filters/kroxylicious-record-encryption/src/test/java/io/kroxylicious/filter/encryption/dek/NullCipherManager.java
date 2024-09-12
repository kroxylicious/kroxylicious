/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;
import java.security.spec.AlgorithmParameterSpec;
import java.util.function.Supplier;

import javax.crypto.Cipher;
import javax.crypto.NullCipher;

import io.kroxylicious.filter.encryption.config.CipherSpec;

public class NullCipherManager implements CipherManager {

    private final byte id;
    private final NullParameters nullParameters;
    private final boolean constantParamsSize;

    record NullParameters(byte[] bytes) implements AlgorithmParameterSpec {

    }

    public NullCipherManager(byte id, boolean constantParamsSize, byte[] paramsBytes) {
        this.id = id;
        this.constantParamsSize = constantParamsSize;
        this.nullParameters = new NullParameters(paramsBytes);
    }

    @Override
    public int requiredNumKeyBits() {
        return 0;
    }

    @Override
    public byte serializedId() {
        return id;
    }

    @Override
    public CipherSpec name() {
        return CipherSpec.CHACHA20_POLY1305; // masquerade as cha cha poly
    }

    @Override
    public long maxEncryptionsPerKey() {
        return Long.MAX_VALUE;
    }

    @Override
    public Cipher newCipher() {
        return new NullCipher();
    }

    @Override
    public Supplier<AlgorithmParameterSpec> paramSupplier() {
        return () -> nullParameters;
    }

    @Override
    public int constantParamsSize() {
        return constantParamsSize ? nullParameters.bytes().length : -1;
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
        parametersBuffer.put(nullParameters.bytes());
    }

    @Override
    public NullParameters readParameters(ByteBuffer parametersBuffer) {
        byte[] gotParamsBytes = new byte[nullParameters.bytes().length];
        parametersBuffer.get(gotParamsBytes);
        return new NullParameters(gotParamsBytes);
    }
}
