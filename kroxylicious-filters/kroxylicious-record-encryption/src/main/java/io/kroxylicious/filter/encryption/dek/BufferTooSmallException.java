/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

/**
 * Indicates that an allocated buffer had too few bytes remaining for an
 * encryption or decryption operation to be completed.
 */
public class BufferTooSmallException extends RuntimeException {
    /**
     * Creates an exception with no message or cause.
     */
    public BufferTooSmallException() {
        super();
    }
}
