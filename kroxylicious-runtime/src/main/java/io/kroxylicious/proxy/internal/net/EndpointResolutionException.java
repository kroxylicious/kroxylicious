/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

/**
 * Signals that an endpoint could not be resolved into a known virtual cluster binding.
 */
public class EndpointResolutionException extends EndpointException {
    /**
     * Creates an endpoint resolution exception.
     *
     * @param message the detail message
     */
    public EndpointResolutionException(String message) {
        super(message);
    }

    /**
     * Creates an endpoint resolution exception.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public EndpointResolutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
