/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Objects;
import java.util.Optional;

import io.netty.handler.ssl.SslContext;

import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.TargetCluster;

/**
 * Routing model for a virtual cluster that forwards directly to a single, statically-configured
 * upstream Kafka cluster.
 * <p>
 * Owns the pre-built {@link #upstreamSslContext()} and {@link #tlsManager()} for that cluster.
 */
public record DirectRouting(String routeName,
                            TargetCluster targetCluster,
                            Optional<SslContext> upstreamSslContext,
                            TlsCredentialSupplierManager tlsManager)
        implements RoutingModel {

    /**
     * Convenience constructor for contexts where TLS resources are not yet resolved
     * (tests, pre-{@code PluginFactoryRegistry} construction). The SSL context defaults
     * to empty and the manager to unconfigured.
     */
    public DirectRouting(String routeName, TargetCluster targetCluster) {
        this(routeName, targetCluster, Optional.empty(), TlsCredentialSupplierManager.unconfigured());
    }

    public DirectRouting {
        Objects.requireNonNull(targetCluster, "targetCluster");
        Objects.requireNonNull(upstreamSslContext, "upstreamSslContext");
        Objects.requireNonNull(tlsManager, "tlsManager");
    }

    @Override
    public TargetCluster targetClusterFor(String routeName) {
        validateRouteName(routeName);
        return targetCluster;
    }

    @Override
    public Optional<SslContext> upstreamSslContextFor(String routeName) {
        validateRouteName(routeName);
        return upstreamSslContext;
    }

    @Override
    public TlsCredentialSupplierManager tlsManagerFor(String routeName) {
        validateRouteName(routeName);
        return tlsManager;
    }

    private void validateRouteName(String routeName) {
        if (!routeName.equals(this.routeName)) {
            throw new IllegalArgumentException("Invalid route name: " + routeName + " for direct routing. Expected: " + this.routeName);
        }
    }

    @Override
    public void close() {
        tlsManager.close();
    }
}
