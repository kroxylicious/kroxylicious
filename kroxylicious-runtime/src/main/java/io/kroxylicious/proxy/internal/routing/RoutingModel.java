/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Optional;

import io.netty.handler.ssl.SslContext;

import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.TargetCluster;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Sealed hierarchy representing how a virtual cluster reaches its upstream Kafka cluster(s).
 * <ul>
 *   <li>{@link DirectRouting} — a single, statically-configured upstream cluster</li>
 *   <li>{@link DynamicRouting} — one or more upstream clusters reached via a named router plugin</li>
 * </ul>
 *
 * <p>Implementations own any TLS resources they hold and must be closed via {@link #close()} when
 * the owning virtual cluster is stopped.
 */
public sealed interface RoutingModel extends AutoCloseable permits DirectRouting, DynamicRouting {
    @Override
    default void close() {
    }

    /**
     * Returns the upstream {@link TargetCluster} for the given route name, or {@code null} if there
     * is no cluster target for that name.
     */
    @Nullable
    TargetCluster targetClusterFor(String routeName);

    /**
     * Returns the pre-built upstream {@link SslContext} for the given route, or
     * {@link Optional#empty()} when the route has no static TLS configuration.
     */
    Optional<SslContext> upstreamSslContextFor(String routeName);

    /**
     * Returns the {@link TlsCredentialSupplierManager} for the given route.
     * Returns the unconfigured singleton when no dynamic TLS credential supplier is configured
     */
    TlsCredentialSupplierManager tlsManagerFor(String routeName);

    /**
     * Closes any TLS resources owned by this routing model (e.g. per-route
     * {@link TlsCredentialSupplierManager} instances). Called by the owning
     * {@code VirtualClusterModel} on shutdown.
     */
    void close();
}
