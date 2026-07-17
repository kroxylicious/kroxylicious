/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Base exception for credential lookup failures.
 * <p>
 * Thrown when a credential lookup operation fails due to service-level issues.
 * Does not indicate that a user was not found (which is represented by a null return value),
 * but rather that the lookup could not be completed.
 * </p>
 * <p>
 * Subclasses provide more specific failure information:
 * </p>
 * <ul>
 *     <li>{@link CredentialServiceUnavailableException} - Service is unavailable</li>
 *     <li>{@link CredentialServiceTimeoutException} - Operation timed out</li>
 * </ul>
 */
public class CredentialLookupException extends Exception {

    /**
     * Constructs a new credential lookup exception with the specified detail message.
     *
     * @param message the detail message
     */
    public CredentialLookupException(@Nullable String message) {
        super(message);
    }

    /**
     * Constructs a new credential lookup exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public CredentialLookupException(@Nullable String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new credential lookup exception with the specified cause.
     *
     * @param cause the cause
     */
    public CredentialLookupException(@Nullable Throwable cause) {
        super(cause);
    }
}
