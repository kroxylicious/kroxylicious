/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

/**
 * The base class for exceptions relating to the use of {@link Dek}s.
 */
public class DekException extends RuntimeException {
    /**
     * Creates an exception with the given message.
     * @param message the detail message.
     */
    public DekException(String message) {
        super(message);
    }

    /**
     * Creates an exception with no message or cause.
     */
    public DekException() {
        super();
    }

    /**
     * Creates an exception with the given cause.
     * @param cause the cause of this exception.
     */
    public DekException(Throwable cause) {
        super(cause);
    }
}
