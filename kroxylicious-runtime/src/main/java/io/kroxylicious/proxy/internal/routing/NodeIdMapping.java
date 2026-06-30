/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

/**
 * Maps between target-cluster node IDs and the virtual node IDs
 * presented to clients. Implementations must be thread-safe.
 * <p>
 * <b>Negative node IDs</b> — the Kafka protocol uses negative node IDs as
 * sentinels with defined semantics, for example {@code -1} means "no leader",
 * "unknown controller", or "no coordinator available". These values must pass
 * through the mapping unchanged; translating them would corrupt the protocol.
 * Implementations must therefore return any negative input unmodified from both
 * {@link #toVirtual} and {@link #fromVirtual(String, int)}.
 */
public sealed interface NodeIdMapping permits BijectiveNodeIdMapping, IdentityNodeIdMapping {

    /**
     * Translates a target-cluster node ID to a virtual node ID.
     * <p>
     * Negative values (Kafka protocol sentinels such as {@code -1} "no leader")
     * are returned unchanged and must not be fed into the translation formula.
     *
     * @param route the route name
     * @param targetNodeId the node ID on the target cluster
     * @return the virtual node ID, or {@code targetNodeId} unchanged if negative
     */
    int toVirtual(String route, int targetNodeId);

    /**
     * Translates a virtual node ID back to its target-cluster node ID,
     * given the route. The route is always supplied by the router via
     * {@link io.kroxylicious.proxy.router.RouterContext#sendRequest}.
     * <p>
     * As with {@link #toVirtual}, negative values are returned unchanged.
     *
     * @param route the route name
     * @param virtualNodeId the virtual node ID
     * @return the target-cluster node ID, or {@code virtualNodeId} unchanged if negative
     */
    int fromVirtual(String route, int virtualNodeId);

    /**
     * Translates a virtual node ID back to its route and target-cluster node ID.
     * Only works for dedicated (one-to-one) mappings where the route can be
     * recovered from the virtual node ID alone.
     * <p>
     * Behaviour is undefined for negative inputs; proxy port node IDs are always
     * non-negative and this method is never called with a sentinel value.
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
