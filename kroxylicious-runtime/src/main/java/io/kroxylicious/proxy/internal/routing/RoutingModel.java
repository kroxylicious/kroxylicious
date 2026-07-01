/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import io.kroxylicious.proxy.config.TargetCluster;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Sealed hierarchy representing how a virtual cluster reaches its upstream Kafka cluster(s).
 * <ul>
 *   <li>{@link DirectRouting} — a single, statically-configured upstream cluster</li>
 *   <li>{@link DynamicRouting} — one or more upstream clusters reached via a named router plugin</li>
 * </ul>
 */
public sealed interface RoutingModel permits DirectRouting, DynamicRouting {

    /**
     * Returns the upstream {@link TargetCluster} for the given route name, or {@code null} if there
     * is no cluster target for that name.
     * <p>
     * For {@link DirectRouting}, {@code routeName} is ignored and the single configured cluster is
     * always returned. For {@link DynamicRouting}, {@code routeName} identifies the route descriptor
     * to look up; returns {@code null} when the name is not found or the route targets a nested
     * router rather than a cluster.
     */
    @Nullable
    TargetCluster targetClusterFor(@Nullable String routeName);
}
