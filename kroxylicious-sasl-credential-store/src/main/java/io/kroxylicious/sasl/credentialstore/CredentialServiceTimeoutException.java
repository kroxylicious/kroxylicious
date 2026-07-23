/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Exception thrown when a credential lookup operation times out.
 * <p>
 * Indicates that the credential lookup took longer than the configured timeout period.
 * This typically indicates either:
 * </p>
 * <ul>
 *     <li>Network latency or congestion</li>
 *     <li>Slow backing service (overloaded database, etc.)</li>
 *     <li>Timeout configuration too aggressive</li>
 * </ul>
 * <p>
 * Implementations should configure reasonable timeouts to prevent authentication from
 * blocking indefinitely whilst avoiding false positives from transient slowness.
 * </p>
 */
public class CredentialServiceTimeoutException extends CredentialLookupException {

    /**
     * Constructs a new credential service timeout exception with the specified detail message.
     *
     * @param message the detail message
     */
    public CredentialServiceTimeoutException(@Nullable String message) {
        super(message);
    }

    /**
     * Constructs a new credential service timeout exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public CredentialServiceTimeoutException(@Nullable String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new credential service timeout exception with the specified cause.
     *
     * @param cause the cause
     */
    public CredentialServiceTimeoutException(@Nullable Throwable cause) {
        super(cause);
    }
}
