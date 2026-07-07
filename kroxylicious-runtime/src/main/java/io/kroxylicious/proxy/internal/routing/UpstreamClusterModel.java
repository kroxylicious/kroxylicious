/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.netty.handler.ssl.SslContext;

import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustOptions;
import io.kroxylicious.proxy.config.tls.TrustProvider;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

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

    public Optional<Tls> tls() {
        return targetCluster.tls();
    }

    public List<HostPort> bootstrapServersList() {
        return targetCluster.bootstrapServersList();
    }

    public HostPort bootstrapServer() {
        return targetCluster.bootstrapServer();
    }

    public boolean usesDynamicTlsCredentials() {
        return tls().map(t -> t.credentialSupplier() != null).orElse(false);
    }

    /**
     * Builds a {@link TlsCredentialSupplierManager} from this cluster's TLS credential supplier
     * configuration, or returns {@link Optional#empty()} when no {@link PluginFactoryRegistry} is
     * available or no credential supplier is configured.
     */
    public Optional<TlsCredentialSupplierManager> buildTlsCredentialSupplierManager(@Nullable PluginFactoryRegistry pfr) {
        if (pfr == null) {
            return Optional.empty();
        }
        return tls()
                .flatMap(t -> Optional.ofNullable(t.credentialSupplier()))
                .map(config -> new TlsCredentialSupplierManager(pfr, config));
    }

    /** Returns a TLS summary string for this cluster's upstream TLS configuration. */
    public String tlsSummary() {
        return generateTlsSummary(tls());
    }

    /** Generates a TLS summary string for an arbitrary TLS configuration. */
    public static String generateTlsSummary(Optional<Tls> tlsToSummarize) {
        var tls = tlsToSummarize.map(t -> Optional.ofNullable(t.trust())
                .map(TrustProvider::trustOptions)
                .map(TrustOptions::toString).orElse("-"))
                .map(options -> " (TLS: " + options + ") ").orElse("");
        var cipherSuitesAllowed = tlsToSummarize.map(t -> Optional.ofNullable(t.cipherSuites())
                .map(AllowDeny::allowed).orElse(Collections.emptyList()))
                .map(allowedCiphers -> " (Allowed Ciphers: " + allowedCiphers + ")").orElse("");
        var cipherSuitesDenied = tlsToSummarize.map(t -> Optional.ofNullable(t.cipherSuites())
                .map(AllowDeny::denied).orElse(Collections.emptySet()))
                .map(deniedCiphers -> " (Denied Ciphers: " + deniedCiphers + ")").orElse("");
        var protocolsAllowed = tlsToSummarize.map(t -> Optional.ofNullable(t.protocols())
                .map(AllowDeny::allowed).orElse(Collections.emptyList()))
                .map(protocols -> " (Allowed Protocols: " + protocols + ")").orElse("");
        var protocolsDenied = tlsToSummarize.map(t -> Optional.ofNullable(t.protocols())
                .map(AllowDeny::denied).orElse(Collections.emptySet()))
                .map(protocols -> " (Denied Protocols: " + protocols + ")").orElse("");

        return tls + cipherSuitesAllowed + cipherSuitesDenied + protocolsAllowed + protocolsDenied;
    }

    @Override
    public void close() {
        tlsManager.close();
    }
}
