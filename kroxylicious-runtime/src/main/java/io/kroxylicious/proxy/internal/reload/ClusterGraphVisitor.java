/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Visitor for nodes in a virtual cluster's graph (VirtualCluster → Router DAG → ClusterDefinitions).
 * <p>
 * Each {@code visitXxx} method returns {@code true} to continue traversal or {@code false}
 * to terminate it early. Default implementations return {@code true}, so only the node types
 * of interest need to be overridden.
 * <p>
 * Implementations accumulate state in fields and expose it via {@link #result()} once
 * the walk has finished. The walker calls methods in traversal order and does not reset
 * visitor state between calls.
 *
 * @param <T> the type of value produced by this visitor after the walk completes
 * @see ClusterGraphWalker
 */
interface ClusterGraphVisitor<T> {

    /**
     * Called once for the virtual cluster that serves as the DAG's entry point.
     *
     * @param vc the virtual cluster
     * @return {@code true} to continue traversal, {@code false} to terminate early
     */
    default boolean visitVirtualCluster(VirtualCluster vc) {
        return true;
    }

    /**
     * Called for each router node encountered during traversal.
     * <p>
     * {@code rd} is {@code null} when the router name is referenced but not present in
     * {@code routersByName}; the visitor may react to the missing definition but cannot
     * recover the router name.
     *
     * @param rd the router's definition, or {@code null} if not found
     * @return {@code true} to continue traversal, {@code false} to terminate early
     */
    default boolean visitRouter(@Nullable RouterDefinition rd) {
        return true;
    }

    /**
     * Called for each named cluster at a route leaf.
     * <p>
     * {@code cd} is {@code null} when the cluster name is referenced but not present in
     * {@code clustersByName}.
     *
     * @param cd the cluster's definition, or {@code null} if not found
     * @return {@code true} to continue traversal, {@code false} to terminate early
     */
    default boolean visitClusterName(@Nullable ClusterDefinition cd) {
        return true;
    }

    /**
     * Returns the value accumulated by this visitor over the course of the walk.
     * Called by the walker immediately after traversal completes (or is terminated early).
     *
     * @return the result produced by this visitor
     */
    T result();
}
