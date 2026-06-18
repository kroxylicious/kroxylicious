/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import org.apache.kafka.common.protocol.Errors;

/**
 * Thrown when coordinator discovery fails (FIND_COORDINATOR returns
 * an error).
 */
class CoordinatorDiscoveryException extends RuntimeException {
    private final Errors error;

    CoordinatorDiscoveryException(Errors error) {
        super("Coordinator discovery failed: " + error);
        this.error = error;
    }

    Errors error() {
        return error;
    }
}
