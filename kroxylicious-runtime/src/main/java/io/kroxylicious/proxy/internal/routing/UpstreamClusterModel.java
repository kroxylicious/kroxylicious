/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import javax.net.ssl.SSLException;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustOptions;
import io.kroxylicious.proxy.config.tls.TrustProvider;
import io.kroxylicious.proxy.internal.tls.NettyKeyProvider;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Runtime representation of an upstream Kafka cluster, bundling its connection target with the
 * TLS resources needed to reach it. Owned by the {@link RoutingModel} implementation that holds it;
 * closed via {@link #close()} when the owning routing model is closed.
 *
 * @param targetCluster the connection target for the upstream cluster
 * @param upstreamSslContext the SSL context used to connect to the upstream cluster, or empty when TLS is not configured
 * @param tlsManager the manager of dynamically-supplied TLS credentials; owned and closed by this record
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public record UpstreamClusterModel(
                                   TargetCluster targetCluster,
                                   Optional<SslContext> upstreamSslContext,
                                   TlsCredentialSupplierManager tlsManager)
        implements AutoCloseable {

    /**
     * The TLS configuration of the target cluster.
     *
     * @return the TLS configuration, or empty when TLS is not configured
     */
    public Optional<Tls> tls() {
        return targetCluster.tls();
    }

    /**
     * The bootstrap servers of the target cluster.
     *
     * @return the bootstrap server addresses
     */
    public List<HostPort> bootstrapServersList() {
        return targetCluster.bootstrapServersList();
    }

    /**
     * The first bootstrap server of the target cluster.
     *
     * @return the bootstrap server address
     */
    public HostPort bootstrapServer() {
        return targetCluster.bootstrapServer();
    }

    /**
     * Whether the target cluster's TLS configuration uses a credential supplier plugin.
     *
     * @return true if TLS credentials are supplied dynamically
     */
    public boolean usesDynamicTlsCredentials() {
        return tls().map(t -> t.credentialSupplier() != null).orElse(false);
    }

    /**
     * Whether connections to the upstream cluster require TLS.
     *
     * @return true if TLS is configured for the upstream cluster
     */
    public boolean requiresTls() {
        return upstreamSslContext().isPresent();
    }

    /**
     * Builds a fully-resolved {@link UpstreamClusterModel} for the given target cluster, constructing
     * the SSL context and TLS credential supplier manager from the cluster's TLS configuration.
     *
     * @param targetCluster the connection target for the upstream cluster
     * @param pfr the plugin factory registry used to instantiate a TLS credential supplier plugin, or null when plugins are unavailable
     * @return the upstream cluster model
     */
    public static UpstreamClusterModel build(TargetCluster targetCluster, @Nullable PluginFactoryRegistry pfr) {
        var sslContext = targetCluster.tls().map(targetClusterTls -> {
            try {
                var sslContextBuilder = Optional.ofNullable(targetClusterTls.key())
                        .map(NettyKeyProvider::new).map(NettyKeyProvider::forClient)
                        .orElse(SslContextBuilder.forClient());
                VirtualClusterModel.configureCipherSuites(sslContextBuilder, targetClusterTls);
                VirtualClusterModel.configureEnabledProtocols(sslContextBuilder, targetClusterTls);
                Optional.ofNullable(targetClusterTls.trust())
                        .map(TrustProvider::trustOptions)
                        .filter(Predicate.not(TrustOptions::forClient))
                        .ifPresent(to -> {
                            throw new IllegalConfigurationException("Cannot apply trust options " + to + " to upstream (client) TLS.)");
                        });
                return VirtualClusterModel.configureTrustProvider(targetClusterTls).apply(sslContextBuilder).build();
            }
            catch (SSLException e) {
                throw new UncheckedIOException(e);
            }
        });
        TlsCredentialSupplierManager mgr = pfr != null
                ? targetCluster.tls()
                        .flatMap(t -> Optional.ofNullable(t.credentialSupplier()))
                        .map(config -> new TlsCredentialSupplierManager(pfr, config))
                        .orElse(TlsCredentialSupplierManager.unconfigured())
                : TlsCredentialSupplierManager.unconfigured();
        return new UpstreamClusterModel(targetCluster, sslContext, mgr);
    }

    @Override
    public void close() {
        tlsManager.close();
    }
}
