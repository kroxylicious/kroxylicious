/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Optional;

import io.netty.handler.ssl.SslContext;

import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.TargetCluster;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Bundles per-connection TLS state for use by {@link ServerConnectionStateMachine}.
 * <p>
 * For non-routed virtual clusters this is populated from the single VC-level configuration.
 * For routed virtual clusters each route supplies its own instance, derived from that
 * route's {@link io.kroxylicious.proxy.internal.routing.RouteDescriptor#targetCluster()}.
 *
 * @param staticSslContext pre-built SSL context for static TLS; empty when TLS is absent or dynamic
 * @param tlsManager credential supplier manager for dynamic TLS; unconfigured when not applicable
 * @param connectionTargetCluster target cluster for this connection; {@code null} when plaintext or unknown
 */
record ConnectionTlsConfig(
                           @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<SslContext> staticSslContext,
                           TlsCredentialSupplierManager tlsManager,
                           @Nullable TargetCluster connectionTargetCluster) {

    static ConnectionTlsConfig plaintext() {
        return new ConnectionTlsConfig(Optional.empty(), TlsCredentialSupplierManager.unconfigured(), null);
    }
}
