/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.model;

import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.tls.NettyKeyProvider;
import io.kroxylicious.proxy.config.tls.NettyTrustProvider;
import io.kroxylicious.proxy.config.tls.PlatformTrustProvider;
import io.kroxylicious.proxy.config.tls.ServerOptions;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.config.tls.TrustProvider;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class VirtualCluster implements ClusterNetworkAddressConfigProvider {
    public static final int DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES = 104857600;
    private final String clusterName;

    private final TargetCluster targetCluster;

    private final Optional<Tls> tls;
    private final boolean logNetwork;

    private final boolean logFrames;

    private final ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider;

    private final Optional<SslContext> upstreamSslContext;

    private final Optional<SslContext> downstreamSslContext;

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualCluster.class);

    public VirtualCluster(String clusterName,
                          TargetCluster targetCluster,
                          ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider,
                          Optional<Tls> tls,
                          boolean logNetwork,
                          boolean logFrames) {
        this.clusterName = clusterName;
        this.tls = tls;
        this.targetCluster = targetCluster;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.clusterNetworkAddressConfigProvider = clusterNetworkAddressConfigProvider;

        validateTLsSettings(clusterNetworkAddressConfigProvider, tls);
        validatePortUsage(clusterNetworkAddressConfigProvider);

        // TODO: https://github.com/kroxylicious/kroxylicious/issues/104 be prepared to reload the SslContext at runtime.
        this.upstreamSslContext = buildUpstreamSslContext();
        this.downstreamSslContext = buildDownstreamSslContext();
        logVirtualClusterSummary(clusterName, targetCluster, clusterNetworkAddressConfigProvider, tls);
    }

    @SuppressWarnings("java:S1874") // the classes are deprecated because we don't want them in the API module
    private static void logVirtualClusterSummary(String clusterName, TargetCluster targetCluster,
                                                 ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider,
                                                 Optional<Tls> tls) {
        try {
            var downstreamTls = tls.map(t -> Optional.ofNullable(t.trust())
                    .map(TrustProvider::serverOptions)
                    .map(ServerOptions::clientAuth)
                    .orElse(TlsClientAuth.NONE))
                    .map(clientAuth -> " (TLS clientAuth: " + clientAuth + ")").orElse("");
            HostPort downstreamBootstrap = clusterNetworkAddressConfigProvider.getClusterBootstrapAddress();
            var upstreamTls = targetCluster.tls().map(tls1 -> " (TLS)").orElse("");
            HostPort upstreamHostPort = targetCluster.bootstrapServersList().get(0);
            LOGGER.info("Virtual Cluster: {}, Downstream {}{} => Upstream {}{}",
                    clusterName, downstreamBootstrap, downstreamTls, upstreamHostPort, upstreamTls);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to log summary for Virtual Cluster: {}", clusterName, e);
        }
    }

    public String getClusterName() {
        return clusterName;
    }

    public TargetCluster targetCluster() {
        return targetCluster;
    }

    public ClusterNetworkAddressConfigProvider getClusterNetworkAddressConfigProvider() {
        return clusterNetworkAddressConfigProvider;
    }

    public boolean isLogNetwork() {
        return logNetwork;
    }

    public boolean isLogFrames() {
        return logFrames;
    }

    public boolean isUseTls() {
        return tls.isPresent();
    }

    public int socketFrameMaxSizeBytes() {
        return DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES;
    }

    @Override
    public String toString() {
        return "VirtualCluster{" +
                "clusterName='" + clusterName + '\'' +
                ", targetCluster=" + targetCluster +
                ", tls=" + tls +
                ", logNetwork=" + logNetwork +
                ", logFrames=" + logFrames +
                ", clusterNetworkAddressConfigProvider=" + clusterNetworkAddressConfigProvider +
                ", upstreamSslContext=" + upstreamSslContext +
                ", downstreamSslContext=" + downstreamSslContext +
                '}';
    }

    @Override
    public HostPort getClusterBootstrapAddress() {
        return clusterNetworkAddressConfigProvider.getClusterBootstrapAddress();
    }

    @Override
    public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
        return clusterNetworkAddressConfigProvider.getBrokerAddress(nodeId);
    }

    @Override
    public Optional<String> getBindAddress() {
        return clusterNetworkAddressConfigProvider.getBindAddress();
    }

    @Override
    public boolean requiresTls() {
        return clusterNetworkAddressConfigProvider.requiresTls();
    }

    @Override
    public Set<Integer> getExclusivePorts() {
        return clusterNetworkAddressConfigProvider.getExclusivePorts();
    }

    @Override
    public Set<Integer> getSharedPorts() {
        return clusterNetworkAddressConfigProvider.getSharedPorts();
    }

    @Override
    public Map<Integer, HostPort> discoveryAddressMap() {
        return clusterNetworkAddressConfigProvider.discoveryAddressMap();
    }

    @Override
    public Integer getBrokerIdFromBrokerAddress(HostPort brokerAddress) {
        return clusterNetworkAddressConfigProvider.getBrokerIdFromBrokerAddress(brokerAddress);
    }

    public Optional<SslContext> getDownstreamSslContext() {
        return downstreamSslContext;
    }

    public Optional<SslContext> getUpstreamSslContext() {
        return upstreamSslContext;
    }

    private Optional<SslContext> buildDownstreamSslContext() {
        return tls.map(tlsConfiguration -> {
            try {
                var sslContextBuilder = Optional.of(tlsConfiguration.key()).map(NettyKeyProvider::new).map(NettyKeyProvider::forServer)
                        .orElseThrow();

                return configureTrustProvider(tlsConfiguration).apply(sslContextBuilder).build();
            }
            catch (SSLException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private Optional<SslContext> buildUpstreamSslContext() {
        return targetCluster.tls().map(targetClusterTls -> {
            try {
                var sslContextBuilder = Optional.ofNullable(targetClusterTls.key()).map(NettyKeyProvider::new).map(NettyKeyProvider::forClient)
                        .orElse(SslContextBuilder.forClient());

                var withTrust = configureTrustProvider(targetClusterTls).apply(sslContextBuilder);

                return withTrust.build();
            }
            catch (SSLException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @NonNull
    private static NettyTrustProvider configureTrustProvider(Tls tlsConfiguration) {
        final TrustProvider trustProvider = Optional.ofNullable(tlsConfiguration.trust()).orElse(PlatformTrustProvider.INSTANCE);
        return new NettyTrustProvider(trustProvider);
    }

    private static void validatePortUsage(ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider) {
        var conflicts = clusterNetworkAddressConfigProvider.getExclusivePorts().stream().filter(p -> clusterNetworkAddressConfigProvider.getSharedPorts().contains(p))
                .collect(Collectors.toSet());
        if (!conflicts.isEmpty()) {
            throw new IllegalStateException(
                    "The set of exclusive ports described by the cluster endpoint provider must be distinct from those described as shared. Intersection: " + conflicts);
        }
    }

    private static void validateTLsSettings(ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider, Optional<Tls> tls) {
        if (clusterNetworkAddressConfigProvider.requiresTls() && (tls.isEmpty() || !tls.get().definesKey())) {
            throw new IllegalStateException("Cluster endpoint provider requires server TLS, but this virtual cluster does not define it.");
        }
    }
}
