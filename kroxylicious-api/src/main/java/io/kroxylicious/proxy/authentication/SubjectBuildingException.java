/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

/**
 * An exception to be thrown if a {@link Subject} cannot be built.
 */
public class SubjectBuildingException extends RuntimeException {
    /**
     * Creates a new exception with the given message.
     * @param message The detail message.
     */
    public SubjectBuildingException(String message) {
        super(message);
    }

    /**
     * Creates a new exception with the given message and cause.
     * @param message The detail message.
     * @param cause The cause.
     */
    public SubjectBuildingException(String message, Throwable cause) {
        super(message, cause);
    }
}
