/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

/**
 * This class is the general class of exceptions produced by failed endpoint operations.
 */
public abstract class EndpointException extends RuntimeException {
    protected EndpointException(String message) {
        super(message);
    }

    protected EndpointException(String message, Throwable cause) {
        super(message, cause);
    }
}
