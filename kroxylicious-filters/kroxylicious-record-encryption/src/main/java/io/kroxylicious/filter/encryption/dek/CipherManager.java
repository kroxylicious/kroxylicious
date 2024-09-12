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

import io.kroxylicious.filter.encryption.common.PersistedIdentifiable;
import io.kroxylicious.filter.encryption.config.CipherSpec;

/**
 * Abstraction for creating cipher instances and managing their parameters.
 */
public interface CipherManager extends PersistedIdentifiable<CipherSpec> {

    int VARIABLE_SIZE_PARAMETERS = -1;

    @Override
    byte serializedId();

    @Override
    CipherSpec name();

    long maxEncryptionsPerKey();

    Cipher newCipher();

    /**
     * Return a supplier of parameters for use with the cipher.
     * The supplier need not be thread-safe.
     */
    Supplier<AlgorithmParameterSpec> paramSupplier();

    /**
     * If the number of bytes required by {@link #writeParameters(ByteBuffer, AlgorithmParameterSpec)}
     * does not depend on the parameters, then returns the number.
     * Otherwise, if the number of bytes required by  {@link #writeParameters(ByteBuffer, AlgorithmParameterSpec)} is variable
     * returns {@link #VARIABLE_SIZE_PARAMETERS}.
     */
    int constantParamsSize();

    /**
     * Return the number of bytes required by {@link #writeParameters(ByteBuffer, AlgorithmParameterSpec)}
     * to serialize the given parameters.
     * If {@link #constantParamsSize()} returns a number >= 0 then this must return the same number.
     */
    int size(AlgorithmParameterSpec parameterSpec);

    /**
     * Serialize the given parameters to the given buffer, which should have at least
     * {@link #size(AlgorithmParameterSpec)} bytes {@linkplain ByteBuffer#remaining() remaining}.
     */
    void writeParameters(
            ByteBuffer parametersBuffer,
            AlgorithmParameterSpec params
    );

    /**
     * Read previously-serialize parameters from the given buffer.
     * The implementation should know how many bytes to read, so the number of
     * {@linkplain ByteBuffer#remaining() remaining} bytes need only be ≥ (not =)
     * the {@link #size(AlgorithmParameterSpec)} at the time the buffer was written.
     */
    AlgorithmParameterSpec readParameters(ByteBuffer parametersBuffer);

    /**
     * Returns the size of the keys (in bits) that this ciphertext manager requires.
     * @return the required key size in bits.
     */
    int requiredNumKeyBits();
}
