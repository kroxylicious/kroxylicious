/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

/**
 * Represents problems interacting with a Key Management System.
 */
public class KmsException extends RuntimeException {
    /**
     * Creates a new exception with no detail message.
     */
    public KmsException() {
    }

    /**
     * Creates a new exception with the given cause.
     * @param cause The cause.
     */
    public KmsException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new exception with the given message.
     * @param message The detail message.
     */
    public KmsException(String message) {
        super(message);
    }

    /**
     * Creates a new exception with the given message and cause.
     * @param message The detail message.
     * @param cause The cause.
     */
    public KmsException(String message, Throwable cause) {
        super(message, cause);
    }
}
