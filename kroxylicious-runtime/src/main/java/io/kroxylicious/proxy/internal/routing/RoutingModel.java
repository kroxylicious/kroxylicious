/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Sealed hierarchy representing how a virtual cluster reaches its upstream Kafka cluster(s).
 * <ul>
 *   <li>{@link DirectRouting} — a single, statically-configured upstream cluster</li>
 *   <li>{@link DynamicRouting} — one or more upstream clusters reached via a named router plugin</li>
 * </ul>
 *
 * <p>Implementations own the {@link UpstreamClusterModel} instances they hold and must release
 * them via {@link #close()} when the owning virtual cluster is stopped.
 */
public sealed interface RoutingModel extends AutoCloseable permits DirectRouting, DynamicRouting {

    /**
     * Returns the {@link UpstreamClusterModel} for the given route name, or {@code null} if the
     * route does not target an upstream cluster (e.g. it targets a nested router).
     */
    @Nullable
    UpstreamClusterModel upstreamClusterFor(String routeName);

    @Override
    void close();
}
