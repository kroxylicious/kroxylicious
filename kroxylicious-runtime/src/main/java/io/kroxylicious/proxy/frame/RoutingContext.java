/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

/**
 * Routing identity attached to a frame as it flows through the pipeline.
 * Each frame independently carries its own routing context, which is robust
 * under fan-out where a Router fires multiple frames for different routes.
 */
public sealed interface RoutingContext {

    /**
     * The route this frame is associated with.
     */
    String route();

    /**
     * Message is being forwarded along a Route, intended for the default broker (implied by the connection)
     */
    record RouteDefaultNode(String route) implements RoutingContext {}

    /**
     * Message is being forwarded along a Route, intended for a specific broker identified by virtual node ID.
     * Used for {@code sendRequest}.
     */
    record RouteTargetNode(String route, int virtualNodeId) implements RoutingContext {}
}
