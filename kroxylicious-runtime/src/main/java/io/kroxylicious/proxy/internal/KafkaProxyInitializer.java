/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SniHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;

import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.config.NettySettings;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.ProxyProtocolMode;
import io.kroxylicious.proxy.internal.codec.KafkaMessageListener;
import io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseEncoder;
import io.kroxylicious.proxy.internal.metrics.MetricEmittingKafkaMessageListener;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointBindingResolver;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.routing.DirectRouting;
import io.kroxylicious.proxy.internal.routing.DynamicRouting;
import io.kroxylicious.proxy.internal.routing.RouteDescriptor;
import io.kroxylicious.proxy.internal.routing.RouteDispatcher;
import io.kroxylicious.proxy.internal.routing.RoutingHandler;
import io.kroxylicious.proxy.internal.routing.RoutingTerminalHandler;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.util.NettyFutures.logFailure;

/**
 * Initializes the Netty pipeline for each accepted client connection: optionally installs
 * PROXY-protocol detection and TLS/SNI handling, resolves the {@link EndpointBinding} for the
 * connection, and then adds the Kafka codecs, filter chain and frontend handler.
 */
public class KafkaProxyInitializer extends ChannelInitializer<Channel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyInitializer.class);

    private static final ChannelInboundHandlerAdapter LOGGING_INBOUND_ERROR_HANDLER = new LoggingInboundErrorHandler();
    @VisibleForTesting
    static final String LOGGING_INBOUND_ERROR_HANDLER_NAME = "loggingInboundErrorHandler";
    /** Pipeline name of the idle-state handler applied before the session has authenticated. */
    public static final String PRE_SESSION_IDLE_HANDLER = "preSessionIdleHandler";

    private final ProxyProtocolMode proxyProtocolMode;
    private final boolean tls;
    private final EndpointBindingResolver bindingResolver;
    private final EndpointReconciler endpointReconciler;
    private final PluginFactoryRegistry pfr;
    private final ApiVersionsServiceImpl apiVersionsService;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<NettySettings> proxyNettySettings;
    private final Counter clientToProxyErrorCounter;
    @Nullable
    private final Long unauthenticatedIdleMillis;
    private final VirtualClusterRegistry virtualClusterRegistry;
    private final ConcurrentHashMap<DynamicRouting, ConcurrentHashMap<Integer, HostPort>> sharedNodeAddressCache = new ConcurrentHashMap<>();

    /**
     * Creates an initializer for client connections accepted on a proxy listening port.
     * @param pfr registry used to instantiate plugins referenced by the configuration
     * @param tls whether the listening port serves TLS connections
     * @param bindingResolver resolver mapping the accepted endpoint (and SNI hostname, if any) to an {@link EndpointBinding}
     * @param endpointReconciler reconciler notified of upstream cluster topology changes
     * @param proxyProtocolMode whether/how the PROXY protocol is accepted on this port
     * @param apiVersionsService service used to intersect API versions with those supported by the proxy
     * @param proxyNettySettings optional Netty tuning settings (e.g. idle timeouts)
     * @param virtualClusterRegistry registry tracking active connections per virtual cluster
     */
    @SuppressWarnings({ "OptionalUsedAsFieldOrParameterType", "java:S107" })
    public KafkaProxyInitializer(PluginFactoryRegistry pfr,
                                 boolean tls,
                                 EndpointBindingResolver bindingResolver,
                                 EndpointReconciler endpointReconciler,
                                 ProxyProtocolMode proxyProtocolMode,
                                 ApiVersionsServiceImpl apiVersionsService,
                                 Optional<NettySettings> proxyNettySettings,
                                 VirtualClusterRegistry virtualClusterRegistry) {
        this.pfr = pfr;
        this.endpointReconciler = endpointReconciler;
        this.proxyProtocolMode = proxyProtocolMode;
        this.tls = tls;
        this.bindingResolver = bindingResolver;
        this.apiVersionsService = apiVersionsService;
        this.proxyNettySettings = proxyNettySettings;
        this.clientToProxyErrorCounter = Metrics.clientToProxyErrorCounter("", null).withTags();
        unauthenticatedIdleMillis = getUnAuthenticatedIdleMillis(this.proxyNettySettings);
        this.virtualClusterRegistry = Objects.requireNonNull(virtualClusterRegistry);
    }

    @Override
    public void initChannel(Channel ch) {
        LOGGER.atTrace()
                .addKeyValue("remote", ch.remoteAddress())
                .addKeyValue("local", ch.localAddress())
                .log("Connection from client");

        var kafkaSession = new KafkaSession(KafkaSessionState.ESTABLISHING);

        if (proxyProtocolMode != ProxyProtocolMode.DISABLED) {
            LOGGER.atDebug().log("Adding PROXY protocol detection handler (mode={})", proxyProtocolMode);
            ch.pipeline().addLast("HaProxyProtocolDetectionHandler",
                    new HaProxyProtocolDetectionHandler(proxyProtocolMode, kafkaSession));
        }

        if (tls) {
            initTlsChannel(ch, kafkaSession);
        }
        else {
            initPlainChannel(ch, kafkaSession);
        }
        addIdleHandlerToPipeline(ch.pipeline());
        addLoggingErrorHandler(ch.pipeline());
    }

    private void initPlainChannel(Channel ch, KafkaSession kafkaSession) {
        ch.pipeline().addLast("plainResolver", new ChannelInboundHandlerAdapter() {
            @SuppressWarnings("java:S1181")
            @Override
            public void channelActive(ChannelHandlerContext ctx) {

                bindingResolver.resolve(ch, null)
                        .handle((binding, t) -> {
                            if (t != null) {
                                ctx.fireExceptionCaught(t);
                                return null;
                            }
                            try {
                                KafkaProxyInitializer.this.initConnection(ch, binding, kafkaSession);
                                ctx.fireChannelActive();
                            }
                            catch (Throwable t1) {
                                ctx.fireExceptionCaught(t1);
                            }
                            finally {
                                ch.pipeline().remove(this);
                            }
                            return null;
                        });
            }
        });
    }

    // deep inheritance tree of SniHandler not something we can fix, throwable is the right choice as we are forwarding it on.
    @SuppressWarnings({ "java:S110", "java:S1181" })
    private void initTlsChannel(Channel ch, KafkaSession kafkaSession) {
        LOGGER.atDebug().log("Adding SSL/SNI handler");
        ch.pipeline().addLast("sniResolver", new SniHandler((sniHostname, promise) -> {
            try {
                var stage = bindingResolver.resolve(ch, sniHostname);
                // completes the netty promise when then resolution completes (success/otherwise).
                stage.handle((binding, t) -> {
                    try {
                        if (t != null) {
                            LOGGER.atWarn()
                                    .addKeyValue("channel", ch)
                                    .addKeyValue("sniHostname", sniHostname)
                                    .addKeyValue("error", t.getMessage())
                                    .log("Exception resolving Virtual Cluster Binding");
                            promise.setFailure(t);
                            return null;
                        }
                        var gateway = binding.endpointGateway();
                        var sslContext = gateway.getDownstreamSslContext();
                        if (sslContext.isEmpty()) {
                            promise.setFailure(new IllegalStateException("Virtual cluster %s does not provide SSL context".formatted(gateway)));
                        }
                        else {
                            KafkaProxyInitializer.this.initConnection(ch, binding, kafkaSession);
                            promise.setSuccess(sslContext.get());
                        }
                    }
                    catch (Throwable t1) {
                        promise.setFailure(t1);
                    }
                    return null;
                });
                return promise;
            }
            catch (Throwable cause) {
                return promise.setFailure(cause);
            }
        }) {

            @Override
            protected void onLookupComplete(ChannelHandlerContext ctx, Future<SslContext> future) throws Exception {
                if (future.isSuccess()) {
                    super.onLookupComplete(ctx, future);
                    ctx.fireChannelActive();
                }
                else {
                    // We've failed to look up the SslContext - this indicates that the SNI hostname was unrecognized
                    // or that the virtual cluster is somehow not configured for TLS. All we can do is close the
                    // connection.
                    clientToProxyErrorCounter.increment();
                    ctx.close().addListener(logFailure(LOGGER, "close after SNI/TLS lookup failure"));
                }

            }
        });
    }

    @VisibleForTesting
    void initConnection(Channel ch, EndpointBinding binding, KafkaSession kafkaSession) {
        var virtualCluster = binding.endpointGateway().virtualCluster();
        var clusterName = virtualCluster.getClusterName();

        TransportSubjectBuilder subjectBuilder = virtualCluster.subjectBuilder(pfr);
        ClientConnectionStateMachine clientConnectionStateMachine = new ClientConnectionStateMachine(binding, subjectBuilder, kafkaSession);
        if (!virtualClusterRegistry.registerConnection(clusterName, clientConnectionStateMachine)) {
            rejectConnection(ch, clusterName);
            return;
        }
        ch.closeFuture().addListener(f -> virtualClusterRegistry.deregisterConnection(clusterName, clientConnectionStateMachine));
        addHandlers(ch, binding, subjectBuilder, clientConnectionStateMachine);
    }

    private void addHandlers(Channel ch, EndpointBinding binding, TransportSubjectBuilder subjectBuilder, ClientConnectionStateMachine clientConnectionStateMachine) {
        var virtualCluster = binding.endpointGateway().virtualCluster();
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.remove(LOGGING_INBOUND_ERROR_HANDLER_NAME);
        if (virtualCluster.isLogNetwork()) {
            pipeline.addLast("networkLogger", new LoggingHandler("io.kroxylicious.proxy.internal.DownstreamNetworkLogger", LogLevel.INFO));
        }

        var dp = new DelegatingDecodePredicate();
        // The decoder, this only cares about the filters
        // because it needs to know whether to decode requests

        var encoderListener = buildMetricsMessageListenerForEncode(binding, virtualCluster);
        var decoderListener = buildMetricsMessageListenerForDecode(binding, virtualCluster);

        KafkaRequestDecoder decoder = new KafkaRequestDecoder(dp, virtualCluster.socketFrameMaxSizeBytes(), apiVersionsService, decoderListener);
        pipeline.addLast("requestDecoder", decoder);
        pipeline.addLast("responseEncoder", new KafkaResponseEncoder(encoderListener));
        pipeline.addLast("saslV0Rejecter", new SaslV0RejectionHandler());
        pipeline.addLast("responseOrderer", new ResponseOrderer());
        if (virtualCluster.isLogFrames()) {
            pipeline.addLast("frameLogger", new LoggingHandler("io.kroxylicious.proxy.internal.DownstreamFrameLogger", LogLevel.INFO));
        }
        var frontendHandler = new KafkaProxyFrontendHandler(
                pfr,
                endpointReconciler,
                apiVersionsService,
                dp,
                subjectBuilder,
                clientConnectionStateMachine,
                proxyNettySettings);

        pipeline.addLast("frontendHandler", frontendHandler);
        switch (virtualCluster.routing()) {
            case DynamicRouting dr -> {
                Router router = virtualCluster.createRouter();
                Map<ApiKeys, String> staticRoutes = router.staticRoutes();
                Set<ApiKeys> decodedKeys = computeDecodedKeysForRouter(staticRoutes, dr.topLevelRouteDescriptors());
                dp.setRouterDecodingRequirements(decodedKeys);

                var sharedAddresses = sharedNodeAddressCache.computeIfAbsent(dr, k -> new ConcurrentHashMap<>());
                var routingHandler = RoutingHandler.topLevel(
                        router,
                        dr.topLevelRouteDescriptors(),
                        staticRoutes,
                        sharedAddresses,
                        clientConnectionStateMachine,
                        dr.nodeIdMapping(),
                        binding.nodeId());
                clientConnectionStateMachine.setRouterActive();
                clientConnectionStateMachine.setUpstreamAddressResolver(
                        virtualNodeId -> routingHandler.resolveRouterNodeAddress(virtualNodeId)
                                .or(() -> endpointReconciler.upstreamAddress(
                                        clientConnectionStateMachine.endpointGateway(), virtualNodeId)));
                pipeline.addLast("routerDispatchHandler", routingHandler);
                pipeline.addLast("routingTerminalHandler", new RoutingTerminalHandler(clientConnectionStateMachine));
            }
            case DirectRouting ignored -> pipeline.addLast("filterChainCompletionHandler",
                    new FilterChainCompletionHandler(clientConnectionStateMachine));
        }
        addLoggingErrorHandler(pipeline);

        LOGGER.atDebug()
                .addKeyValue("channelId", ch::toString)
                .addKeyValue("pipeline", pipeline)
                .log("Initial pipeline");
    }

    /**
     * Only exclude an API key from decoding when all paths between the VC and the target
     * cluster are statically routed. At init time we don't have nested router instances,
     * so routes targeting nested routers are not provably fully static and must remain
     * decoded. API keys whose responses carry node IDs are always decoded for translation.
     */
    static Set<ApiKeys> computeDecodedKeysForRouter(Map<ApiKeys, String> staticRoutes,
                                                    Map<String, RouteDescriptor> routeDescriptors) {
        Set<ApiKeys> decodedKeys = EnumSet.allOf(ApiKeys.class);
        if (!staticRoutes.isEmpty()) {
            for (var entry : staticRoutes.entrySet()) {
                RouteDescriptor rd = routeDescriptors.get(entry.getValue());
                if (rd != null && rd.targetsCluster()) {
                    decodedKeys.remove(entry.getKey());
                }
            }
        }
        decodedKeys.addAll(RouteDispatcher.NODE_ID_TRANSLATION_APIS);
        return decodedKeys;
    }

    private KafkaMessageListener buildMetricsMessageListenerForDecode(EndpointBinding binding, VirtualClusterModel virtualCluster) {
        var clusterName = virtualCluster.getClusterName();
        var nodeId = binding.nodeId();
        var clientToProxyMessageCounterProvider = Metrics.clientToProxyMessageCounterProvider(clusterName, nodeId);
        var clientToProxyMessageSizeDistributionProvider = Metrics.clientToProxyMessageSizeDistributionProvider(clusterName, nodeId);

        return new MetricEmittingKafkaMessageListener(clientToProxyMessageCounterProvider, clientToProxyMessageSizeDistributionProvider);
    }

    private static MetricEmittingKafkaMessageListener buildMetricsMessageListenerForEncode(EndpointBinding binding, VirtualClusterModel virtualCluster) {
        var clusterName = virtualCluster.getClusterName();
        var nodeId = binding.nodeId();
        var proxyToClientMessageCounterProvider = Metrics.proxyToClientMessageCounterProvider(clusterName, nodeId);
        var proxyToClientMessageSizeDistributionProvider = Metrics.proxyToClientMessageSizeDistributionProvider(clusterName, nodeId);
        return new MetricEmittingKafkaMessageListener(proxyToClientMessageCounterProvider,
                proxyToClientMessageSizeDistributionProvider);
    }

    private void rejectConnection(Channel ch, String clusterName) {
        LOGGER.atInfo()
                .addKeyValue("virtualCluster", clusterName)
                .log("Rejecting new connection - virtual cluster is draining");
        ch.close().addListener(logFailure(LOGGER, "close rejected connection during virtual cluster drain"));
    }

    private static void addLoggingErrorHandler(ChannelPipeline pipeline) {
        pipeline.addLast(LOGGING_INBOUND_ERROR_HANDLER_NAME, LOGGING_INBOUND_ERROR_HANDLER);
    }

    private void addIdleHandlerToPipeline(ChannelPipeline pipeline) {
        if (Objects.nonNull(unauthenticatedIdleMillis)) {
            pipeline.addFirst(PRE_SESSION_IDLE_HANDLER, new IdleStateHandler(0, 0, unauthenticatedIdleMillis, TimeUnit.MILLISECONDS));
        }
    }

    @Nullable
    @CheckReturnValue
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Long getUnAuthenticatedIdleMillis(Optional<NettySettings> nettySettings) {
        return nettySettings.flatMap(NettySettings::unauthenticatedIdleTimeout).map(Duration::toMillis).orElse(null);
    }

    @Sharable
    @VisibleForTesting
    static class LoggingInboundErrorHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOGGER.atWarn()
                    .setCause(LOGGER.isDebugEnabled() ? cause : null)
                    .log(LOGGER.isDebugEnabled()
                            ? "An exceptionCaught() event was caught by the error handler {}: {}"
                            : "An exceptionCaught() event was caught by the error handler {}: {}. Increase log level to DEBUG for stacktrace",
                            cause.getClass().getSimpleName(), cause.getMessage());
        }
    }
}
