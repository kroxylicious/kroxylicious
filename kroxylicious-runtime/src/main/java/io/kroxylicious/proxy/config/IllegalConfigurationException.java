/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

/**
 * Signals that the configuration is syntactically/semantically correct but illegal
 * by some policy. For example if a configuration switches on a test-only feature
 * but the proxy does not have test-only configuration loading enabled.
 */
public class IllegalConfigurationException extends RuntimeException {
    public IllegalConfigurationException(String message) {
        super(message);
    }
}
