/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Optional;

import io.netty.handler.ssl.SslContext;

import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.internal.routing.UpstreamClusterModel;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Bundles per-connection TLS state for use by {@link ServerConnectionStateMachine}.
 *
 * @param staticSslContext pre-built SSL context for static TLS; empty when TLS is absent or dynamic
 * @param tlsManager credential supplier manager for dynamic TLS; unconfigured when not applicable
 * @param connectionTls TLS configuration for this connection; {@code null} when plaintext or unknown
 */
record ConnectionTlsConfig(
                           @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<SslContext> staticSslContext,
                           TlsCredentialSupplierManager tlsManager,
                           @Nullable Tls connectionTls) {

    static ConnectionTlsConfig plaintext() {
        return new ConnectionTlsConfig(Optional.empty(), TlsCredentialSupplierManager.unconfigured(), null);
    }

    static ConnectionTlsConfig from(@Nullable UpstreamClusterModel model) {
        if (model == null) {
            return plaintext();
        }
        return new ConnectionTlsConfig(model.upstreamSslContext(), model.tlsManager(), model.tls().orElse(null));
    }
}
