/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.file;

/**
 * Thrown when an operation on a proxy SCRAM credential file fails (e.g. the file cannot be
 * created, read, or written, or a referenced user cannot be found).
 */
public class ScramCredentialFileException extends RuntimeException {

    /**
     * Creates an exception with the given failure message.
     *
     * @param message description of the failure
     */
    public ScramCredentialFileException(String message) {
        super(message);
    }

    /**
     * Creates an exception with the given failure message and cause.
     *
     * @param message description of the failure
     * @param cause the underlying cause
     */
    public ScramCredentialFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
