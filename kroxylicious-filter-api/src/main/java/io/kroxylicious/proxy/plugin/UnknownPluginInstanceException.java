/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

/**
 * A reference to a plugin instance could not be resolved by the proxy runtime.
 */
public class UnknownPluginInstanceException extends RuntimeException {

    public UnknownPluginInstanceException(String message) {
        super(message);
    }

    public UnknownPluginInstanceException(String message, Throwable cause) {
        super(message, cause);
    }
}
