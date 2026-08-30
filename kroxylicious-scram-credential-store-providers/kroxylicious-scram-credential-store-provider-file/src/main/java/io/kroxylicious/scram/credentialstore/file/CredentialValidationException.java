/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.file;

/**
 * Thrown when credential input validation fails (e.g. password too short, invalid username).
 */
public class CredentialValidationException extends RuntimeException {

    /**
     * Creates an exception with the given validation failure message.
     *
     * @param message description of the validation failure
     */
    public CredentialValidationException(String message) {
        super(message);
    }

    /**
     * Creates an exception with the given validation failure message and cause.
     *
     * @param message description of the validation failure
     * @param cause the underlying cause
     */
    public CredentialValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
