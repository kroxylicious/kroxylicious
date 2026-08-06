/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

/**
 * Thrown when a KMS instance is passed reference to key that it does not manage.
 */
public class UnknownKeyException extends KmsException {
    /**
     * Creates a new exception with no detail message.
     */
    public UnknownKeyException() {
        super();
    }

    /**
     * Creates a new exception with the given message.
     * @param message The detail message.
     */
    public UnknownKeyException(String message) {
        super(message);
    }
}
