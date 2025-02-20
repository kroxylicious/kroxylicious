/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.model;

import java.io.UncheckedIOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.NettyKeyProvider;
import io.kroxylicious.proxy.config.tls.NettyTrustProvider;
import io.kroxylicious.proxy.config.tls.PlatformTrustProvider;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustOptions;
import io.kroxylicious.proxy.config.tls.TrustProvider;
import io.kroxylicious.proxy.internal.net.EndpointListener;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class VirtualClusterModel {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterModel.class);
    public static final int DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES = 104857600;

    private final String clusterName;

    private final TargetCluster targetCluster;

    private final boolean logNetwork;

    private final boolean logFrames;

    private final Map<String, EndpointListener> listeners = new HashMap<>();

    private final List<NamedFilterDefinition> filters;

    private final Optional<SslContext> upstreamSslContext;

    public VirtualClusterModel(String clusterName,
                               TargetCluster targetCluster,
                               boolean logNetwork,
                               boolean logFrames,
                               @NonNull List<NamedFilterDefinition> filters) {
        this.clusterName = clusterName;
        this.targetCluster = targetCluster;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.filters = filters;

        // TODO: https://github.com/kroxylicious/kroxylicious/issues/104 be prepared to reload the SslContext at runtime.
        this.upstreamSslContext = buildUpstreamSslContext();
    }

    public void addListener(String name, ClusterNetworkAddressConfigProvider provider, Optional<Tls> tls) {
        validateTLsSettings(provider, tls);
        validatePortUsage(provider);
        listeners.put(name, new VirtualClusterListenerModel(this, provider, tls));
    }

    public TargetCluster targetCluster() {
        return targetCluster;
    }

    public String getClusterName() {
        return clusterName;
    }

    public boolean isLogNetwork() {
        return logNetwork;
    }

    public boolean isLogFrames() {
        return logFrames;
    }

    public int socketFrameMaxSizeBytes() {
        return DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES;
    }

    @Override
    public String toString() {
        return "VirtualCluster{" +
                "clusterName='" + clusterName + '\'' +
                ", targetCluster=" + targetCluster +
                ", listeners=" + listeners +
                ", logNetwork=" + logNetwork +
                ", logFrames=" + logFrames +
                ", upstreamSslContext=" + upstreamSslContext +
                '}';
    }

    private Optional<SslContext> buildUpstreamSslContext() {
        return targetCluster.tls().map(targetClusterTls -> {
            try {
                var sslContextBuilder = Optional.ofNullable(targetClusterTls.key()).map(NettyKeyProvider::new).map(NettyKeyProvider::forClient)
                        .orElse(SslContextBuilder.forClient());

                configureCipherSuites(sslContextBuilder, targetClusterTls);
                configureEnabledProtocols(sslContextBuilder, targetClusterTls);

                Optional.ofNullable(targetClusterTls.trust())
                        .map(TrustProvider::trustOptions)
                        .filter(Predicate.not(TrustOptions::forClient))
                        .ifPresent(to -> {
                            throw new IllegalConfigurationException("Cannot apply trust options " + to + " to upstream (client) TLS.)");
                        });

                var withTrust = configureTrustProvider(targetClusterTls).apply(sslContextBuilder);

                return withTrust.build();
            }
            catch (SSLException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private static void configureCipherSuites(SslContextBuilder sslContextBuilder, Tls tlsConfiguration) {
        Optional.ofNullable(tlsConfiguration.cipherSuites())
                .ifPresent(ciphers -> sslContextBuilder.ciphers(
                        tlsConfiguration.cipherSuites().allowed(),
                        new DenyCipherSuiteFilter(tlsConfiguration.cipherSuites().denied())));
    }

    private static void configureEnabledProtocols(SslContextBuilder sslContextBuilder, Tls tlsConfiguration) {
        var protocols = Optional.ofNullable(tlsConfiguration.protocols());
        var defaultProtocols = Arrays.stream(getDefaultSSLParameters().getProtocols()).toList();
        var supportedProtocols = Arrays.stream(getSupportedSSLParameters().getProtocols()).toList();

        protocols.ifPresent(allowDeny -> {
            var allowedProtocols = protocols.map(AllowDeny::allowed).orElse(defaultProtocols);

            var deniedProtocols = protocols.map(AllowDeny::denied).orElse(Set.of());

            allowedProtocols.stream()
                    .filter(Predicate.not(supportedProtocols::contains))
                    .forEach(unsupportedProtocol -> LOGGER.warn("Ignoring allowed protocol '{}' as it is not recognized by this platform (supported protocols: {})",
                            unsupportedProtocol, supportedProtocols));

            deniedProtocols.stream()
                    .filter(Predicate.not(supportedProtocols::contains))
                    .forEach(unsupportedProtocol -> LOGGER.warn("Ignoring denied protocol '{}' as it is not recognized by this platform (supported protocols: {})",
                            unsupportedProtocol, supportedProtocols));

            var protocolsToUse = allowedProtocols.stream()
                    .filter(supportedProtocols::contains)
                    .filter(Predicate.not(deniedProtocols::contains))
                    .toList();

            if (!protocolsToUse.isEmpty()) {
                sslContextBuilder.protocols(protocolsToUse);
            }
            else {
                throw new IllegalConfigurationException(
                        "The protocols configuration you have in place has resulted in no protocols being set. Allowed: " + allowedProtocols + ", Denied: "
                                + deniedProtocols);
            }
        });
    }

    public Optional<SslContext> getUpstreamSslContext() {
        return upstreamSslContext;
    }

    private static SSLParameters getDefaultSSLParameters() {
        try {
            return SSLContext.getDefault().getDefaultSSLParameters();
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static SSLParameters getSupportedSSLParameters() {
        try {
            return SSLContext.getDefault().getSupportedSSLParameters();
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
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

    public @NonNull List<NamedFilterDefinition> getFilters() {
        return filters;
    }

    public Map<String, EndpointListener> listeners() {
        return Collections.unmodifiableMap(listeners);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class VirtualClusterListenerModel implements EndpointListener {
        private final VirtualClusterModel virtualCluster;
        private final ClusterNetworkAddressConfigProvider provider;
        private final Optional<Tls> tls;
        private final Optional<SslContext> downstreamSslContext;

        public VirtualClusterListenerModel(VirtualClusterModel virtualCluster, ClusterNetworkAddressConfigProvider provider, Optional<Tls> tls) {
            this.virtualCluster = virtualCluster;
            this.provider = provider;
            this.tls = tls;

            this.downstreamSslContext = buildDownstreamSslContext();

        }

        @Override
        public VirtualClusterModel virtualCluster() {
            return virtualCluster;
        }

        public ClusterNetworkAddressConfigProvider getClusterNetworkAddressConfigProvider() {
            return provider;
        }

        public Optional<Tls> tls() {
            return tls;
        }

        @Override
        public String toString() {
            return "VirtualClusterListenerModel[" +
                    "virtualCluster=" + virtualCluster + ", " +
                    "provider=" + provider + ", " +
                    "tls=" + tls + ']';
        }

        public HostPort getClusterBootstrapAddress() {
            return getClusterNetworkAddressConfigProvider().getClusterBootstrapAddress();
        }

        @Override
        public TargetCluster targetCluster() {
            return virtualCluster.targetCluster();
        }

        public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
            return getClusterNetworkAddressConfigProvider().getBrokerAddress(nodeId);
        }

        public Optional<String> getBindAddress() {
            return getClusterNetworkAddressConfigProvider().getBindAddress();
        }

        public boolean requiresTls() {
            return getClusterNetworkAddressConfigProvider().requiresTls();
        }

        @Override
        public Set<Integer> getExclusivePorts() {
            return getClusterNetworkAddressConfigProvider().getExclusivePorts();
        }

        @Override
        public Set<Integer> getSharedPorts() {
            return getClusterNetworkAddressConfigProvider().getSharedPorts();
        }

        public Map<Integer, HostPort> discoveryAddressMap() {
            return getClusterNetworkAddressConfigProvider().discoveryAddressMap();
        }

        public Integer getBrokerIdFromBrokerAddress(HostPort brokerAddress) {
            return getClusterNetworkAddressConfigProvider().getBrokerIdFromBrokerAddress(brokerAddress);
        }

        @Override
        public Optional<SslContext> getDownstreamSslContext() {
            return downstreamSslContext;
        }

        private Optional<SslContext> buildDownstreamSslContext() {
            return tls.map(tlsConfiguration -> {
                try {
                    var sslContextBuilder = Optional.of(tlsConfiguration.key()).map(NettyKeyProvider::new).map(NettyKeyProvider::forServer)
                            .orElseThrow();

                    configureCipherSuites(sslContextBuilder, tlsConfiguration);
                    configureEnabledProtocols(sslContextBuilder, tlsConfiguration);

                    return configureTrustProvider(tlsConfiguration).apply(sslContextBuilder).build();
                }
                catch (SSLException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        public HostPort getAdvertisedBrokerAddress(int nodeId) {
            return getClusterNetworkAddressConfigProvider().getAdvertisedBrokerAddress(nodeId);
        }

        public boolean isUseTls() {
            return tls.isPresent();
        }

    }
}
