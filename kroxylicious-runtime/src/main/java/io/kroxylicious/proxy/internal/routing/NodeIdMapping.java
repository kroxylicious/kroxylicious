/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

/**
 * Maps between target-cluster node IDs and the virtual node IDs
 * presented to clients. Implementations must be thread-safe.
 */
public sealed interface NodeIdMapping permits BijectiveNodeIdMapping, IdentityNodeIdMapping {

    /**
     * Translates a target-cluster node ID to a virtual node ID.
     *
     * @param route the route name
     * @param targetNodeId the node ID on the target cluster
     * @return the virtual node ID
     */
    int toVirtual(String route, int targetNodeId);

    /**
     * Translates a virtual node ID back to its target-cluster node ID,
     * given the route. The route is always supplied by the router via
     * {@link io.kroxylicious.proxy.router.RouterContext#sendRequestToNode}.
     *
     * @param route the route name
     * @param virtualNodeId the virtual node ID
     * @return the target-cluster node ID
     */
    int fromVirtual(String route, int virtualNodeId);

    /**
     * Translates a virtual node ID back to its route and target-cluster node ID.
     * Only works for dedicated (one-to-one) mappings where the route can be
     * recovered from the virtual node ID alone.
     *
     * @param virtualNodeId the virtual node ID
     * @return the route name and target node ID
     */
    RouteAndNode fromVirtual(int virtualNodeId);

    /**
     * A route name and target-cluster node ID pair.
     */
    record RouteAndNode(String route, int targetNodeId) {}
}
