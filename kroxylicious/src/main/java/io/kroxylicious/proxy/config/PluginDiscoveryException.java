/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

/**
 * Problems with plugin interfaces implementations.
 * Typically, these represent errors by the plugin author or user, such as not applying plugin annotations correctly.
 */
public class PluginDiscoveryException extends RuntimeException {
    public PluginDiscoveryException(String message) {
        super(message);
    }

}
