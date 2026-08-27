/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import org.apache.kafka.common.protocol.Errors;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Signals that a router answered an out-of-band request with a Kafka protocol error
 * ({@link RouterResponseImpl.RespondWithError}) rather than a normal response, so the
 * request's promise is completed exceptionally with this instead of a response body.
 */
class RoutingDispatchException extends RuntimeException {

    private final Errors error;

    RoutingDispatchException(Errors error, @Nullable String message) {
        super(message);
        this.error = error;
    }

    Errors error() {
        return error;
    }
}
