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

/**
 * Runtime representation of an upstream Kafka cluster, bundling its connection target with the
 * TLS resources needed to reach it. Owned by the {@link RoutingModel} implementation that holds it;
 * closed via {@link #close()} when the owning routing model is closed.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public record UpstreamClusterModel(
                                   TargetCluster targetCluster,
                                   Optional<SslContext> upstreamSslContext,
                                   TlsCredentialSupplierManager tlsManager)
        implements AutoCloseable {

    @Override
    public void close() {
        tlsManager.close();
    }
}
