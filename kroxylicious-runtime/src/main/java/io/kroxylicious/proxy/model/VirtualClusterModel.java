/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.model;

import java.io.UncheckedIOException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilderService;
import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.TransportSubjectBuilderConfig;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.PlatformTrustProvider;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustOptions;
import io.kroxylicious.proxy.config.tls.TrustProvider;
import io.kroxylicious.proxy.internal.filter.impl.TopicNameCacheFilter;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.routing.DirectRouting;
import io.kroxylicious.proxy.internal.routing.DynamicRouting;
import io.kroxylicious.proxy.internal.routing.RoutingModel;
import io.kroxylicious.proxy.internal.routing.UpstreamClusterModel;
import io.kroxylicious.proxy.internal.subject.DefaultTransportSubjectBuilderService;
import io.kroxylicious.proxy.internal.tls.NettyKeyProvider;
import io.kroxylicious.proxy.internal.tls.NettyTrustProvider;
import io.kroxylicious.proxy.internal.tls.SslContextBuildException;
import io.kroxylicious.proxy.internal.util.StableKroxyliciousLinkGenerator;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.NodeIdentificationStrategy;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Runtime representation of a virtual cluster: its name, target Kafka cluster, gateways,
 * TLS configuration, and the components whose lifecycle is bound to this VC.
 *
 * <h2>Owned resources</h2>
 * The VCM owns and is responsible for closing two per-VC components directly:
 * <ul>
 *   <li>{@link FilterChainFactory} — the filter chain factory for <em>this</em> VC. Each VCM
 *       has its own FCF.</li>
 *   <li>{@link TlsCredentialSupplierManager} — the TLS credential supplier for this VC.</li>
 * </ul>
 * For VCs that use dynamic routing, the {@link io.kroxylicious.proxy.internal.routing.DynamicRouting}
 * instance carries and owns the {@link io.kroxylicious.proxy.bootstrap.RouterChainFactory}; the VCM
 * closes it via the routing model.
 *
 * <h2>Lifecycle</h2>
 * The VCM is created when the VC is configured (or reconfigured via hot-reload), and closed
 * via {@link #close()} when the VC's lifecycle reaches {@code Stopped}. The
 * {@code VirtualClusterRegistry} drives the close.
 *
 * <p>{@link #close()} is idempotent: the underlying components guard against double-close,
 * so accidental redundant close calls are safe.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class VirtualClusterModel implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterModel.class);
    public static final int DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES = 104857600;

    private final String clusterName;

    private final RoutingModel routing;

    private final boolean logNetwork;

    private final boolean logFrames;

    private final Map<String, VirtualClusterGatewayModel> gateways = new HashMap<>();

    private final List<NamedFilterDefinition> filters;

    private final CacheConfiguration topicNameCacheConfig;
    private final @Nullable TransportSubjectBuilderConfig transportSubjectBuilderConfig;
    private final Duration drainTimeout;
    // lazily initialize to delay statistics registration until after the meter registry has been configured
    @Nullable
    private TopicNameCacheFilter topicNameCacheFilter = null;

    /**
     * The filter chain factory for <em>this</em> virtual cluster. Owned by the VCM — its
     * lifetime is tied to this VCM's lifetime, closed when {@link #close()} is called by
     * {@code VirtualClusterRegistry} on transition into {@code Stopped}.
     */
    private final FilterChainFactory filterChainFactory;

    @VisibleForTesting
    public VirtualClusterModel(String clusterName,
                               TargetCluster targetCluster,
                               boolean logNetwork,
                               boolean logFrames,
                               List<NamedFilterDefinition> filters) {
        this(clusterName, new DirectRouting(clusterName + "Upstream", targetCluster), logNetwork, logFrames, filters,
                new CacheConfiguration(null, null, null), null, Duration.ofSeconds(10), null);
    }

    @SuppressWarnings("java:S107")
    public VirtualClusterModel(String clusterName,
                               RoutingModel routing,
                               boolean logNetwork,
                               boolean logFrames,
                               List<NamedFilterDefinition> filters,
                               CacheConfiguration topicNameCacheConfig,
                               @Nullable TransportSubjectBuilderConfig transportSubjectBuilderConfig,
                               Duration drainTimeout,
                               @Nullable PluginFactoryRegistry pluginFactoryRegistry) {
        this.clusterName = Objects.requireNonNull(clusterName);
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.filters = filters;
        this.topicNameCacheConfig = topicNameCacheConfig;
        this.transportSubjectBuilderConfig = transportSubjectBuilderConfig;
        this.drainTimeout = Objects.requireNonNull(drainTimeout);
        this.routing = Objects.requireNonNull(routing);
        this.filterChainFactory = pluginFactoryRegistry != null
                ? new FilterChainFactory(pluginFactoryRegistry, filters)
                : new FilterChainFactory(null, List.of());
    }

    /**
     * Returns this VC's filter chain factory. The returned factory is alive for the lifetime
     * of this VCM; closing the VCM (via {@link #close()}, driven by
     * {@code VirtualClusterRegistry} on transition into {@code Stopped}) also closes the FCF.
     * Callers should not retain the reference past the VC's lifetime.
     */
    public FilterChainFactory filterChainFactory() {
        return filterChainFactory;
    }

    public Router createRouter() {
        if (!(routing instanceof DynamicRouting dr)) {
            throw new IllegalStateException("Virtual cluster '" + clusterName + "' does not use a router");
        }
        return dr.createRouter(clusterName);
    }

    public Duration drainTimeout() {
        return drainTimeout;
    }

    public RoutingModel routing() {
        return routing;
    }

    public void logVirtualClusterSummary() {
        LOGGER.atInfo()
                .addKeyValue("virtualCluster", clusterName)
                .log("Gateway summary");

        gateways.forEach((name, gateway) -> {
            var downstreamBootstrap = gateway.getClusterBootstrapAddress();
            var downstreamTlsSummary = UpstreamClusterModel.generateTlsSummary(gateway.getTls());

            var logBuilder = LOGGER.atInfo()
                    .addKeyValue("gateway", name)
                    .addKeyValue("downstream", downstreamBootstrap + downstreamTlsSummary);
            logBuilder = switch (routing) {
                case DirectRouting dr -> logBuilder.addKeyValue("upstream",
                        dr.upstreamCluster().bootstrapServersList() + dr.upstreamCluster().tlsSummary());
                case DynamicRouting ignored -> logBuilder.addKeyValue("upstream", "(via router)");
            };
            logBuilder.log("Gateway configuration");
        });
    }

    public void addGateway(String name, NodeIdentificationStrategy nodeIdentificationStrategy, Optional<Tls> tls) {
        gateways.put(name, new VirtualClusterGatewayModel(this, nodeIdentificationStrategy, tls, name));
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
                ", routing=" + routing +
                ", gateways=" + gateways +
                ", logNetwork=" + logNetwork +
                ", logFrames=" + logFrames +
                '}';
    }

    /**
     * Returns the {@link UpstreamClusterModel} for a specific route, or {@code null} if the route
     * does not target an upstream cluster (e.g. it targets a nested router, or no TLS has been
     * resolved yet because no {@code PluginFactoryRegistry} was provided).
     */
    @Nullable
    public UpstreamClusterModel getUpstreamClusterForRoute(String routeName) {
        return routing.upstreamClusterFor(routeName);
    }

    /**
     * Closes resources associated with this virtual cluster — the TLS credential supplier
     * manager(s) held by the routing model and the {@link FilterChainFactory}. Called by
     * {@code VirtualClusterRegistry} on lifecycle transition into {@code Stopped}. Safe to call
     * multiple times — the FCF's underlying {@code Wrapper.close} is idempotent via an internal
     * {@code AtomicBoolean}, and {@code TlsCredentialSupplierManager.close} tolerates re-entry.
     */
    @Override
    public void close() {
        // Suppress exceptions so each component still gets a chance to close; surface the
        // first failure at the end so callers see something rather than nothing.
        RuntimeException firstFailure = null;
        try {
            routing.close();
        }
        catch (RuntimeException e) {
            firstFailure = e;
        }
        try {
            filterChainFactory.close();
        }
        catch (RuntimeException e) {
            if (firstFailure == null) {
                firstFailure = e;
            }
            else {
                firstFailure.addSuppressed(e);
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    /**
     * Checks if this virtual cluster uses dynamic TLS credential supplier.
     *
     * @return true if a credential supplier is configured
     */
    public boolean usesDynamicTlsCredentials() {
        return routing instanceof DirectRouting dr && dr.upstreamCluster().usesDynamicTlsCredentials();
    }

    public static NettyTrustProvider configureTrustProvider(Tls tlsConfiguration) {
        final TrustProvider trustProvider = Optional.ofNullable(tlsConfiguration.trust()).orElse(PlatformTrustProvider.INSTANCE);
        return new NettyTrustProvider(trustProvider);
    }

    /**
     * Builds an upstream (client-side) {@link SslContext} from the given target cluster's TLS
     * configuration. Returns {@link Optional#empty()} when {@code targetCluster} is {@code null}
     * or has no TLS configuration.
     */
    public static Optional<SslContext> buildUpstreamSslContextFor(@Nullable TargetCluster targetCluster) {
        if (targetCluster == null) {
            return Optional.empty();
        }
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

    public static void configureCipherSuites(SslContextBuilder sslContextBuilder, Tls tlsConfiguration) {
        Optional.ofNullable(tlsConfiguration.cipherSuites())
                .ifPresent(ciphers -> sslContextBuilder.ciphers(
                        tlsConfiguration.cipherSuites().allowed(),
                        new DenyCipherSuiteFilter(tlsConfiguration.cipherSuites().denied())));
    }

    public static void configureEnabledProtocols(SslContextBuilder sslContextBuilder, Tls tlsConfiguration) {
        var protocols = Optional.ofNullable(tlsConfiguration.protocols());
        var defaultProtocols = Arrays.stream(getDefaultSSLParameters().getProtocols()).toList();
        var supportedProtocols = Arrays.stream(getSupportedSSLParameters().getProtocols()).toList();

        protocols.ifPresent(allowDeny -> {
            var allowedProtocols = protocols.map(AllowDeny::allowed).orElse(defaultProtocols);

            var deniedProtocols = protocols.map(AllowDeny::denied).orElse(Set.of());

            allowedProtocols.stream()
                    .filter(Predicate.not(supportedProtocols::contains))
                    .forEach(unsupportedProtocol -> LOGGER.atWarn()
                            .addKeyValue("unsupportedProtocol", unsupportedProtocol)
                            .addKeyValue("supportedProtocols", supportedProtocols)
                            .log("Ignoring allowed protocol as it is not recognized by this platform"));

            deniedProtocols.stream()
                    .filter(Predicate.not(supportedProtocols::contains))
                    .forEach(unsupportedProtocol -> LOGGER.atWarn()
                            .addKeyValue("unsupportedProtocol", unsupportedProtocol)
                            .addKeyValue("supportedProtocols", supportedProtocols)
                            .log("Ignoring denied protocol as it is not recognized by this platform"));

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
            throw new SslContextBuildException(e);
        }
    }

    private static SSLParameters getSupportedSSLParameters() {
        try {
            return SSLContext.getDefault().getSupportedSSLParameters();
        }
        catch (NoSuchAlgorithmException e) {
            throw new SslContextBuildException(e);
        }
    }

    public List<NamedFilterDefinition> getFilters() {
        return filters;
    }

    public Map<String, EndpointGateway> gateways() {
        return Collections.unmodifiableMap(gateways);
    }

    public TransportSubjectBuilder subjectBuilder(PluginFactoryRegistry pfr) {
        var pf = pfr.pluginFactory(TransportSubjectBuilderService.class);
        String type;
        Object config;
        if (this.transportSubjectBuilderConfig == null) {
            type = DefaultTransportSubjectBuilderService.class.getName();
            config = new DefaultTransportSubjectBuilderService.Config(List.of());
        }
        else {
            type = this.transportSubjectBuilderConfig.type();
            config = this.transportSubjectBuilderConfig.config();
        }
        Class<?> configType = pf.configType(type);
        if (config != null && !configType.isInstance(config)) {
            throw new PluginConfigurationException("SubjectBuilder " + type + " accepts config of type " +
                    configType.getName() + " but provided with config of type " + config.getClass().getName());
        }
        TransportSubjectBuilderService subjectBuilderService = pf.pluginInstance(type);
        subjectBuilderService.initialize(config);
        return subjectBuilderService.build();
    }

    public TopicNameCacheFilter getTopicNameCacheFilter() {
        if (topicNameCacheFilter == null) {
            topicNameCacheFilter = new TopicNameCacheFilter(topicNameCacheConfig, clusterName);
        }
        return topicNameCacheFilter;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class VirtualClusterGatewayModel implements EndpointGateway {
        private final VirtualClusterModel virtualCluster;
        private final NodeIdentificationStrategy nodeIdentificationStrategy;
        private final Optional<Tls> tls;
        private final Optional<SslContext> downstreamSslContext;
        private final String name;

        @VisibleForTesting
        VirtualClusterGatewayModel(VirtualClusterModel virtualCluster, NodeIdentificationStrategy nodeIdentificationStrategy, Optional<Tls> tls, String name) {
            this.virtualCluster = virtualCluster;
            this.nodeIdentificationStrategy = nodeIdentificationStrategy;
            this.tls = tls;
            this.name = name;
            validatePortUsage(nodeIdentificationStrategy);
            validateTLsSettings(nodeIdentificationStrategy, tls);
            this.downstreamSslContext = buildDownstreamSslContext();
        }

        private void validatePortUsage(NodeIdentificationStrategy nodeIdentificationStrategy) {
            // This validation seems misplaced.
            var conflicts = nodeIdentificationStrategy.getExclusivePorts().stream().filter(p -> nodeIdentificationStrategy.getSharedPorts().contains(p))
                    .collect(Collectors.toSet());
            if (!conflicts.isEmpty()) {
                throw new IllegalStateException(
                        "The set of exclusive ports described by the Node Identification Strategy must be distinct from those described as shared. Intersection: "
                                + conflicts);
            }
        }

        private void validateTLsSettings(NodeIdentificationStrategy nodeIdentificationStrategy, Optional<Tls> tls) {
            if (nodeIdentificationStrategy.requiresServerNameIndication() && (tls.isEmpty() || !tls.get().definesKey())) {
                throw new IllegalStateException(
                        "Node Identification Strategy requires ServerNameIndication, but virtual cluster gateway '%s' does not configure TLS and provide a certificate for the server"
                                .formatted(name()));
            }
        }

        @Override
        public VirtualClusterModel virtualCluster() {
            return virtualCluster;
        }

        private NodeIdentificationStrategy getNodeIdentificationStrategy() {
            return nodeIdentificationStrategy;
        }

        @Override
        public HostPort getClusterBootstrapAddress() {
            return getNodeIdentificationStrategy().getClusterBootstrapAddress();
        }

        @Override
        public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
            return getNodeIdentificationStrategy().getBrokerAddress(nodeId);
        }

        @Override
        public Optional<String> getBindAddress() {
            return getNodeIdentificationStrategy().getBindAddress();
        }

        @Override
        public boolean requiresServerNameIndication() {
            return getNodeIdentificationStrategy().requiresServerNameIndication();
        }

        @Override
        public Set<Integer> getExclusivePorts() {
            return getNodeIdentificationStrategy().getExclusivePorts();
        }

        @Override
        public Set<Integer> getSharedPorts() {
            return getNodeIdentificationStrategy().getSharedPorts();
        }

        @Override
        public Map<Integer, HostPort> discoveryAddressMap() {
            return getNodeIdentificationStrategy().discoveryAddressMap();
        }

        @Override
        public @Nullable Integer getBrokerIdFromBrokerAddress(HostPort brokerAddress) {
            return getNodeIdentificationStrategy().getBrokerIdFromBrokerAddress(brokerAddress);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public HostPort getAdvertisedBrokerAddress(int nodeId) {
            return getNodeIdentificationStrategy().getAdvertisedBrokerAddress(nodeId);
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
                if (tlsConfiguration.key() == null) {
                    throw new IllegalConfigurationException(
                            "Virtual cluster '%s', gateway '%s': 'tls' object is missing the mandatory attribute 'key'. See %s for details"
                                    .formatted(virtualCluster.getClusterName(), name(),
                                            StableKroxyliciousLinkGenerator.INSTANCE.errorLink(StableKroxyliciousLinkGenerator.CLIENT_TLS)));
                }
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
                    "nodeIdentificationStrategy=" + nodeIdentificationStrategy + ", " +
                    "tls=" + isUseTls() + ']';
        }
    }
}
