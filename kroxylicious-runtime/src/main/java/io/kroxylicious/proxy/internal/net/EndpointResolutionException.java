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
    public EndpointResolutionException(String message) {
        super(message);
    }

    public EndpointResolutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
