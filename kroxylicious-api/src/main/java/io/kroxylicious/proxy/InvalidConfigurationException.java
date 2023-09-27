/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

/**
 * Thrown when configuration is invalid
 */
public class InvalidConfigurationException extends RuntimeException {

    /**
     * Creates InvalidConfigurationException
     * @param message message
     */
    public InvalidConfigurationException(String message) {
        super(message);
    }

    /**
     * Creates InvalidConfigurationException
     * @param message message
     * @param cause cause
     */
    public InvalidConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
