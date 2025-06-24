/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import java.util.Objects;

/**
 * Thrown when a plugin configuration is invalid
 */
public class PluginConfigurationException extends RuntimeException {

    /**
     * Initializes a new instance
     * @param message message
     */
    public PluginConfigurationException(String message) {
        super(Objects.requireNonNull(message));
    }

    /**
     * Initializes a new instance
     * @param message message
     * @param cause cause
     */
    public PluginConfigurationException(String message, Throwable cause) {
        super(Objects.requireNonNull(message), cause);
    }
}
