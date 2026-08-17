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

    /**
     * The value returned by {@link #constantParamsSize()} when the size of the serialized
     * parameters depends on the parameters themselves.
     */
    int VARIABLE_SIZE_PARAMETERS = -1;

    @Override
    byte serializedId();

    @Override
    CipherSpec name();

    /**
     * Returns the maximum number of encryption operations which may safely be
     * performed using a single key with this cipher.
     * @return the maximum number of encryption operations per key.
     */
    long maxEncryptionsPerKey();

    /**
     * Creates a new {@link Cipher} instance for this cipher.
     * @return a new cipher instance.
     */
    Cipher newCipher();

    /**
     * Return a supplier of parameters for use with the cipher.
     * The supplier need not be thread-safe.
     * @return a supplier of cipher parameters.
     */
    Supplier<AlgorithmParameterSpec> paramSupplier();

    /**
     * If the number of bytes required by {@link #writeParameters(ByteBuffer, AlgorithmParameterSpec)}
     * does not depend on the parameters, then returns the number.
     * Otherwise, if the number of bytes required by  {@link #writeParameters(ByteBuffer, AlgorithmParameterSpec)} is variable
     * returns {@link #VARIABLE_SIZE_PARAMETERS}.
     * @return the number of bytes required to serialize the parameters, or {@link #VARIABLE_SIZE_PARAMETERS}.
     */
    int constantParamsSize();

    /**
     * Return the number of bytes required by {@link #writeParameters(ByteBuffer, AlgorithmParameterSpec)}
     * to serialize the given parameters.
     * If {@link #constantParamsSize()} returns a number >= 0 then this must return the same number.
     * @param parameterSpec the parameters to be serialized.
     * @return the number of bytes required to serialize the given parameters.
     */
    int size(AlgorithmParameterSpec parameterSpec);

    /**
     * Serialize the given parameters to the given buffer, which should have at least
     * {@link #size(AlgorithmParameterSpec)} bytes {@linkplain ByteBuffer#remaining() remaining}.
     * @param parametersBuffer the buffer to serialize the parameters to.
     * @param params the parameters to be serialized.
     */
    void writeParameters(
                         ByteBuffer parametersBuffer,
                         AlgorithmParameterSpec params);

    /**
     * Read previously-serialize parameters from the given buffer.
     * The implementation should know how many bytes to read, so the number of
     * {@linkplain ByteBuffer#remaining() remaining} bytes need only be ≥ (not =)
     * the {@link #size(AlgorithmParameterSpec)} at the time the buffer was written.
     * @param parametersBuffer the buffer to read the parameters from.
     * @return the deserialized parameters.
     */
    AlgorithmParameterSpec readParameters(ByteBuffer parametersBuffer);

    /**
     * Returns the size of the keys (in bits) that this ciphertext manager requires.
     * @return the required key size in bits.
     */
    int requiredNumKeyBits();
}
