/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

/**
 * Thrown when the {@link AuthorizationFilter} encounters an error which prevents it
 * from enforcing authorization.
 */
public class AuthorizationException extends RuntimeException {
    /**
     * Creates the exception.
     * @param message The detail message.
     */
    public AuthorizationException(String message) {
        super(message);
    }

    /**
     * Creates the exception.
     * @param message The detail message.
     * @param cause The underlying cause.
     */
    public AuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }
}
