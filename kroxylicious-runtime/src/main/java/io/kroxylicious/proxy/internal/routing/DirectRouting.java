/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Objects;
import java.util.Optional;

import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.TargetCluster;

/**
 * Routing model for a virtual cluster that forwards directly to a single, statically-configured
 * upstream Kafka cluster.
 * <p>
 * Owns the {@link UpstreamClusterModel} for that cluster and closes it when {@link #close()} is called.
 */
public record DirectRouting(String routeName, UpstreamClusterModel upstreamCluster)
        implements RoutingModel {

    /**
     * Test-only constructor: creates a routing model with no TLS resources resolved.
     * Production code should always supply a fully-built {@link UpstreamClusterModel}.
     */
    public DirectRouting(String routeName, TargetCluster targetCluster) {
        this(routeName, new UpstreamClusterModel(targetCluster, Optional.empty(), TlsCredentialSupplierManager.unconfigured()));
    }

    public DirectRouting {
        Objects.requireNonNull(routeName, "routeName");
        Objects.requireNonNull(upstreamCluster, "upstreamCluster");
    }

    @Override
    public UpstreamClusterModel upstreamClusterFor(String routeName) {
        if (!routeName.equals(this.routeName)) {
            throw new NoUpstreamClusterForRouteException("no upstream cluster for " + routeName + " direct routing handles fixed route name: " + this.routeName);
        }
        return upstreamCluster;
    }

    @Override
    public void close() {
        upstreamCluster.close();
    }

    public static String routeName(String virtualClusterName) {
        return virtualClusterName + "Upstream";
    }
}
