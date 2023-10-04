/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

/**
 * A reference to a plugin type could not be resolved by the proxy runtime.
 */
public class UnknownPluginTypeException extends RuntimeException {
    public UnknownPluginTypeException(String message) {
        super(message);
    }

    public UnknownPluginTypeException(String message, Throwable cause) {
        super(message, cause);
    }
}
