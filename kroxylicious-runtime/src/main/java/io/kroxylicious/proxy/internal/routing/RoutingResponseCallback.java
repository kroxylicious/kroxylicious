/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

/**
 * Callback for receiving responses from backend servers when a router is active.
 */
@FunctionalInterface
public interface RoutingResponseCallback {
    /**
     * Handle a response from a backend server.
     *
     * @param msg the response message
     * @return {@code true} if this callback handled the response (dynamically routed),
     *         {@code false} if the caller should forward it to the client via the normal path
     */
    boolean onResponse(Object msg);
}
