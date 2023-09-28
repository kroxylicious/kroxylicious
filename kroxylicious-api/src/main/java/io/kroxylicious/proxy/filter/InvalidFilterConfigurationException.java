/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

/**
 * Thrown when filter configuration is invalid
 */
public class InvalidFilterConfigurationException extends RuntimeException {

    /**
     * Creates InvalidConfigurationException
     * @param message message
     */
    public InvalidFilterConfigurationException(String message) {
        super(message);
    }

    /**
     * Creates InvalidConfigurationException
     * @param message message
     * @param cause cause
     */
    public InvalidFilterConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
