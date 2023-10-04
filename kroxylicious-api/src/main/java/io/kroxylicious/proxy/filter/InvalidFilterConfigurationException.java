/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Thrown when filter configuration is invalid
 */
public class InvalidFilterConfigurationException extends RuntimeException {

    /**
     * Creates InvalidConfigurationException
     * @param message message
     */
    public InvalidFilterConfigurationException(@NonNull String message) {
        super(Objects.requireNonNull(message));
    }

    /**
     * Creates InvalidConfigurationException
     * @param message message
     * @param cause cause
     */
    public InvalidFilterConfigurationException(@NonNull String message, Throwable cause) {
        super(Objects.requireNonNull(message), cause);
    }
}
