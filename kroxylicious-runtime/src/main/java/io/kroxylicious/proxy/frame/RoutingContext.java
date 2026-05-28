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
     * Forward to the route's bootstrap server.
     * Used for static routes and bootstrap sends.
     */
    record RouteBootstrap(String route) implements RoutingContext {}

    /**
     * Forward to a specific broker identified by virtual node ID.
     * Used for {@code sendRequestToNode}.
     */
    record RouteTargetNode(String route, int virtualNodeId) implements RoutingContext {}
}
