/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

/**
 * Signals that an endpoint could not be bound due to an error condition.
 */
public class EndpointBindingException extends EndpointException {
    /**
     * Creates an endpoint binding exception.
     *
     * @param message the detail message
     */
    public EndpointBindingException(String message) {
        super(message);
    }

    /**
     * Creates an endpoint binding exception.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public EndpointBindingException(String message, Throwable cause) {
        super(message, cause);
    }
}
