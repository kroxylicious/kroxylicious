/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

/**
 * Thrown when an SSL context cannot be built from the configured TLS key or trust material.
 */
public class SslContextBuildException extends RuntimeException {

    /**
     * Constructor.
     *
     * @param cause the underlying cause.
     */
    public SslContextBuildException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     *
     * @param message the detail message.
     * @param cause the underlying cause.
     */
    public SslContextBuildException(String message, Throwable cause) {
        super(message, cause);
    }
}
