/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.tls;

/**
 * Represents problems applying TLS configuration to an HTTP client, such as
 * invalid key material, unreadable key or trust stores, or restrictions that
 * leave no TLS protocols or cipher suites available.
 */
public class SslConfigurationException extends RuntimeException {
    /**
     * Creates a new exception with the given cause.
     * @param cause The cause.
     */
    public SslConfigurationException(Exception cause) {
        super(cause);
    }

    /**
     * Creates a new exception with the given message.
     * @param message The detail message.
     */
    public SslConfigurationException(String message) {
        super(message);
    }

    /**
     * Creates a new exception with the given message and cause.
     * @param message The detail message.
     * @param cause The cause.
     */
    public SslConfigurationException(String message, Exception cause) {
        super(message, cause);
    }
}
