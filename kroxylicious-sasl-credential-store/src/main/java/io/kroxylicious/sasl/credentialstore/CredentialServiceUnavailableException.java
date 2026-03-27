/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Exception thrown when the credential service is unavailable.
 * <p>
 * Indicates that the backing credential store (database, LDAP directory, file system, etc.)
 * cannot be reached or is not responding. This is typically a transient failure that may
 * resolve itself, though authentication cannot proceed until the service is available.
 * </p>
 * <p>
 * Examples include:
 * </p>
 * <ul>
 *     <li>Database connection failures</li>
 *     <li>LDAP server unreachable</li>
 *     <li>Network partitions</li>
 *     <li>Service temporarily down for maintenance</li>
 * </ul>
 */
public class CredentialServiceUnavailableException extends CredentialLookupException {

    /**
     * Constructs a new credential service unavailable exception with the specified detail message.
     *
     * @param message the detail message
     */
    public CredentialServiceUnavailableException(@Nullable String message) {
        super(message);
    }

    /**
     * Constructs a new credential service unavailable exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public CredentialServiceUnavailableException(@Nullable String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new credential service unavailable exception with the specified cause.
     *
     * @param cause the cause
     */
    public CredentialServiceUnavailableException(@Nullable Throwable cause) {
        super(cause);
    }
}
