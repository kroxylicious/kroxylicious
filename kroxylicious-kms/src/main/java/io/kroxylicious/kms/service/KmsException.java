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
    public KmsException() {
    }

    public KmsException(Throwable cause) {
        super(cause);
    }

    public KmsException(String message) {
        super(message);
    }

    public KmsException(String message, Throwable cause) {
        super(message, cause);
    }
}
