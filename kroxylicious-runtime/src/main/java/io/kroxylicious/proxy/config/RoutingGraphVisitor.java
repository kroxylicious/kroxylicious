/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Visitor for nodes in a virtual cluster's routing graph (VirtualCluster → Router DAG → ClusterDefinitions).
 * <p>
 * Naming convention: {@code visit*} methods are one-shot notifications (the node has no
 * children to recurse into); {@code enterRouter} is the recursive router callback.
 * There is no matching {@code leaveRouter} — it was omitted because a partially-called
 * leave (not called on revisits) would break the enter/leave contract.
 * <p>
 * Each method returns {@code true} to continue traversal or {@code false} to terminate
 * early. Default implementations return {@code true}, so only the node types of interest
 * need to be overridden.
 * <p>
 * A {@link WalkContext} is supplied to every callback, carrying the edge that led to the
 * current node, whether this is a {@linkplain WalkContext#isFirstVisit() first or revisit},
 * and the {@linkplain WalkContext#path() router-name path} from the walk entry point.
 * <p>
 * Implementations accumulate state in fields and expose it via {@link #result()} once
 * the walk has finished. The walker calls methods in traversal order; visitor state is
 * never reset between callbacks.
 *
 * @param <T> the type of value produced by this visitor after the walk completes
 * @see RoutingGraphWalker
 */
public interface RoutingGraphVisitor<T> {

    /**
     * Called once for the virtual cluster that serves as the routing graph's entry point.
     * <p>
     * {@link WalkContext#currentRoute()} and {@link WalkContext#sourceRouter()} are
     * {@code null}; {@link WalkContext#path()} is empty; {@link WalkContext#isFirstVisit()}
     * is {@code true}.
     *
     * @param vc  the virtual cluster
     * @param ctx context for this traversal step
     * @return {@code true} to continue traversal, {@code false} to terminate early
     */
    default boolean visitVirtualCluster(VirtualCluster vc, WalkContext ctx) {
        return true;
    }

    /**
     * Called for each router node encountered during traversal.
     * <p>
     * {@code rd} is {@code null} when the router name is referenced but not present in
     * the router map; the visitor may read the referenced name from
     * {@link WalkContext#currentRoute()}.
     * <p>
     * When {@link WalkContext#isFirstVisit()} is {@code false}, the walker will not recurse
     * into this router's children — the router is already on the current DFS path, forming
     * a cycle.
     *
     * @param rd  the router's definition, or {@code null} if the referenced name is not
     *            in the router map
     * @param ctx context for this traversal step
     * @return {@code true} to continue traversal, {@code false} to terminate early
     */
    default boolean enterRouter(@Nullable RouterDefinition rd, WalkContext ctx) {
        return true;
    }

    /**
     * Called for each named cluster at a route leaf.
     * <p>
     * {@code cd} is {@code null} when the cluster name is referenced but not present in
     * the cluster map.
     *
     * @param cd  the cluster's definition, or {@code null} if the referenced name is not
     *            in the cluster map
     * @param ctx context for this traversal step
     * @return {@code true} to continue traversal, {@code false} to terminate early
     */
    default boolean visitClusterName(@Nullable ClusterDefinition cd, WalkContext ctx) {
        return true;
    }

    /**
     * Returns the value accumulated by this visitor over the course of the walk.
     * Called by the walker immediately after traversal completes (or is terminated early).
     * May return {@code null} when {@code T} is {@link Void}.
     *
     * @return the result produced by this visitor
     */
    @Nullable
    T result();
}
