/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

/**
 * Thrown when the body of an HTTP response from Azure could not be understood,
 * for example because it is empty or is not valid JSON.
 */
public class MalformedResponseBodyException extends RuntimeException {
    /**
     * Creates the exception.
     *
     * @param message the detail message.
     * @param cause the underlying cause.
     */
    public MalformedResponseBodyException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates the exception.
     *
     * @param message the detail message.
     */
    public MalformedResponseBodyException(String message) {
        super(message);
    }
}
