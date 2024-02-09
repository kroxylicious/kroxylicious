/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;

/**
 * Abstracts how buffers are allocated for encryption.
 * This allows either a real allocation or the slicing of an already allocated buffer.
 */
@FunctionalInterface
public interface EncryptAllocator {
    /**
     * @param size The size of the required buffer
     * @return A buffer. If this has less than {@code size} bytes remaining
     * then the encryption will ultimately fail with {@link BufferTooSmallException}
     */
    ByteBuffer buffer(int size);
}
