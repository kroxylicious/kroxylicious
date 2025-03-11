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
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class VirtualClusterModel {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterModel.class);
    public static final int DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES = 104857600;

    private final String clusterName;

    private final TargetCluster targetCluster;

    private final boolean logNetwork;

    private final boolean logFrames;

    private final Map<String, VirtualClusterGatewayModel> gateways = new HashMap<>();

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

    public void logVirtualClusterSummary() {
        var upstreamHostPort = targetCluster.bootstrapServersList().get(0);
        var upstreamTlsSummary = generateTlsSummary(targetCluster.tls());

        LOGGER.info("Virtual Cluster '{}' - gateway summary", clusterName);

        gateways.forEach((name, gateway) -> {
            var downstreamBootstrap = gateway.getClusterBootstrapAddress();
            var downstreamTlsSummary = generateTlsSummary(gateway.getTls());

            LOGGER.info("Gateway: {}, Downstream {}{} => Upstream {}{}",
                    name, downstreamBootstrap, downstreamTlsSummary, upstreamHostPort, upstreamTlsSummary);
        });
    }

    private static String generateTlsSummary(Optional<Tls> tlsToSummarize) {
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

    public void addGateway(String name, ClusterNetworkAddressConfigProvider provider, Optional<Tls> tls) {
        gateways.put(name, new VirtualClusterGatewayModel(this, provider, tls, name));
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
        return "VirtualClusterModel{" +
                "clusterName='" + clusterName + '\'' +
                ", targetCluster=" + targetCluster +
                ", gateways=" + gateways +
                ", logNetwork=" + logNetwork +
                ", logFrames=" + logFrames +
                ", upstreamSslContext=" + upstreamSslContext +
                '}';
    }

    public Optional<SslContext> getUpstreamSslContext() {
        return upstreamSslContext;
    }

    @NonNull
    private static NettyTrustProvider configureTrustProvider(Tls tlsConfiguration) {
        final TrustProvider trustProvider = Optional.ofNullable(tlsConfiguration.trust()).orElse(PlatformTrustProvider.INSTANCE);
        return new NettyTrustProvider(trustProvider);
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

    public @NonNull List<NamedFilterDefinition> getFilters() {
        return filters;
    }

    public Map<String, EndpointGateway> gateways() {
        return Collections.unmodifiableMap(gateways);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class VirtualClusterGatewayModel implements EndpointGateway {
        private final VirtualClusterModel virtualCluster;
        private final ClusterNetworkAddressConfigProvider provider;
        private final Optional<Tls> tls;
        private final Optional<SslContext> downstreamSslContext;
        private final String name;

        @VisibleForTesting
        VirtualClusterGatewayModel(VirtualClusterModel virtualCluster, ClusterNetworkAddressConfigProvider provider, Optional<Tls> tls, String name) {
            this.virtualCluster = virtualCluster;
            this.provider = provider;
            this.tls = tls;
            this.name = name;
            validatePortUsage(provider);
            validateTLsSettings(provider, tls);
            this.downstreamSslContext = buildDownstreamSslContext();
        }

        private void validatePortUsage(ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider) {
            // This validation seems misplaced.
            var conflicts = clusterNetworkAddressConfigProvider.getExclusivePorts().stream().filter(p -> clusterNetworkAddressConfigProvider.getSharedPorts().contains(p))
                    .collect(Collectors.toSet());
            if (!conflicts.isEmpty()) {
                throw new IllegalStateException(
                        "The set of exclusive ports described by the cluster endpoint provider must be distinct from those described as shared. Intersection: "
                                + conflicts);
            }
        }

        private void validateTLsSettings(ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider, Optional<Tls> tls) {
            if (clusterNetworkAddressConfigProvider.requiresServerNameIndication() && (tls.isEmpty() || !tls.get().definesKey())) {
                throw new IllegalStateException(
                        "Cluster endpoint provider requires ServerNameIndication, but virtual cluster gateway '%s' does not configure TLS and provide a certificate for the server"
                                .formatted(name()));
            }
        }

        @Override
        public VirtualClusterModel virtualCluster() {
            return virtualCluster;
        }

        private ClusterNetworkAddressConfigProvider getClusterNetworkAddressConfigProvider() {
            return provider;
        }

        @Override
        public HostPort getClusterBootstrapAddress() {
            return getClusterNetworkAddressConfigProvider().getClusterBootstrapAddress();
        }

        @Override
        public TargetCluster targetCluster() {
            return virtualCluster.targetCluster();
        }

        @Override
        public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
            return getClusterNetworkAddressConfigProvider().getBrokerAddress(nodeId);
        }

        @Override
        public Optional<String> getBindAddress() {
            return getClusterNetworkAddressConfigProvider().getBindAddress();
        }

        @Override
        public boolean requiresServerNameIndication() {
            return getClusterNetworkAddressConfigProvider().requiresServerNameIndication();
        }

        @Override
        public Set<Integer> getExclusivePorts() {
            return getClusterNetworkAddressConfigProvider().getExclusivePorts();
        }

        @Override
        public Set<Integer> getSharedPorts() {
            return getClusterNetworkAddressConfigProvider().getSharedPorts();
        }

        @Override
        public Map<Integer, HostPort> discoveryAddressMap() {
            return getClusterNetworkAddressConfigProvider().discoveryAddressMap();
        }

        @Override
        public Integer getBrokerIdFromBrokerAddress(HostPort brokerAddress) {
            return getClusterNetworkAddressConfigProvider().getBrokerIdFromBrokerAddress(brokerAddress);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public HostPort getAdvertisedBrokerAddress(int nodeId) {
            return getClusterNetworkAddressConfigProvider().getAdvertisedBrokerAddress(nodeId);
        }

        @Override
        public boolean isUseTls() {
            return tls.isPresent();
        }

        @Override
        public Optional<SslContext> getDownstreamSslContext() {
            return downstreamSslContext;
        }

        public Optional<Tls> getTls() {
            return tls;
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

        @Override
        public String toString() {
            return "VirtualClusterGatewayModel[" +
                    "name=" + name + ", " +
                    "virtualCluster=" + virtualCluster.getClusterName() + ", " +
                    "provider=" + provider + ", " +
                    "tls=" + isUseTls() + ']';
        }
    }
}
