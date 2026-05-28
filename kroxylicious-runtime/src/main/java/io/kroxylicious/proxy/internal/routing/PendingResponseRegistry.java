/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

/**
 * Abstraction for registering and deregistering pending routing responses.
 * Implementations manage the mapping from routing correlation ID to
 * the pending response future.
 */
interface PendingResponseRegistry {
    void register(int correlationId, PendingResponse pendingResponse);

    void deregister(int correlationId);
}
