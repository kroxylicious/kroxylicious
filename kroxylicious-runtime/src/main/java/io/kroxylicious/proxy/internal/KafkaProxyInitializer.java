/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SniHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.NettySettings;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.internal.codec.KafkaMessageListener;
import io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseEncoder;
import io.kroxylicious.proxy.internal.metrics.MetricEmittingKafkaMessageListener;
import io.kroxylicious.proxy.internal.net.Endpoint;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointBindingResolver;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaProxyInitializer extends ChannelInitializer<Channel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyInitializer.class);

    private static final ChannelInboundHandlerAdapter LOGGING_INBOUND_ERROR_HANDLER = new LoggingInboundErrorHandler();
    @VisibleForTesting
    static final String LOGGING_INBOUND_ERROR_HANDLER_NAME = "loggingInboundErrorHandler";
    public static final String PRE_SESSION_IDLE_HANDLER = "preSessionIdleHandler";

    private final boolean haproxyProtocol;
    private final boolean tls;
    private final EndpointBindingResolver bindingResolver;
    private final EndpointReconciler endpointReconciler;
    private final PluginFactoryRegistry pfr;
    // Shared mutable reference to FilterChainFactory - enables hot reload of filter configurations
    private final AtomicReference<FilterChainFactory> filterChainFactoryRef;
    private final ApiVersionsServiceImpl apiVersionsService;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<NettySettings> proxyNettySettings;
    private final Counter clientToProxyErrorCounter;
    @Nullable
    private final Long unauthenticatedIdleMillis;
    private final ConnectionTracker connectionTracker;
    private final ConnectionDrainManager connectionDrainManager;
    private final InFlightMessageTracker inFlightTracker;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public KafkaProxyInitializer(AtomicReference<FilterChainFactory> filterChainFactoryRef,
                                 PluginFactoryRegistry pfr,
                                 boolean tls,
                                 EndpointBindingResolver bindingResolver,
                                 EndpointReconciler endpointReconciler,
                                 boolean haproxyProtocol,
                                 ApiVersionsServiceImpl apiVersionsService,
                                 Optional<NettySettings> proxyNettySettings,
                                 ConnectionTracker connectionTracker,
                                 ConnectionDrainManager connectionDrainManager,
                                 InFlightMessageTracker inFlightTracker) {
        this.pfr = pfr;
        this.endpointReconciler = endpointReconciler;
        this.haproxyProtocol = haproxyProtocol;
        this.tls = tls;
        this.bindingResolver = bindingResolver;
        this.filterChainFactoryRef = filterChainFactoryRef;
        this.apiVersionsService = apiVersionsService;
        this.proxyNettySettings = proxyNettySettings;
        this.clientToProxyErrorCounter = Metrics.clientToProxyErrorCounter("", null).withTags();
        unauthenticatedIdleMillis = getUnAuthenticatedIdleMillis(this.proxyNettySettings);
        this.connectionTracker = connectionTracker;
        this.connectionDrainManager = connectionDrainManager;
        this.inFlightTracker = inFlightTracker;
    }

    @Override
    public void initChannel(Channel ch) {
        LOGGER.trace("Connection from {} to my address {}", ch.remoteAddress(), ch.localAddress());

        if (tls) {
            initTlsChannel(ch);
        }
        else {
            initPlainChannel(ch);
        }
        addIdleHandlerToPipeline(ch.pipeline());
        addLoggingErrorHandler(ch.pipeline());
    }

    private void initPlainChannel(Channel ch) {
        ch.pipeline().addLast("plainResolver", new ChannelInboundHandlerAdapter() {
            @SuppressWarnings("java:S1181")
            @Override
            public void channelActive(ChannelHandlerContext ctx) {

                bindingResolver.resolve(Endpoint.createEndpoint(ch, tls), null)
                        .handle((binding, t) -> {
                            if (t != null) {
                                ctx.fireExceptionCaught(t);
                                return null;
                            }
                            try {
                                KafkaProxyInitializer.this.addHandlers(ch, binding);
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
    private void initTlsChannel(Channel ch) {
        LOGGER.debug("Adding SSL/SNI handler");
        ch.pipeline().addLast("sniResolver", new SniHandler((sniHostname, promise) -> {
            try {
                Endpoint endpoint = Endpoint.createEndpoint(ch, tls);
                var stage = bindingResolver.resolve(endpoint, sniHostname);
                // completes the netty promise when then resolution completes (success/otherwise).
                stage.handle((binding, t) -> {
                    try {
                        if (t != null) {
                            LOGGER.warn("Exception resolving Virtual Cluster Binding for endpoint {} and sniHostname {}: {}", endpoint, sniHostname, t.getMessage());
                            promise.setFailure(t);
                            return null;
                        }
                        var gateway = binding.endpointGateway();
                        var sslContext = gateway.getDownstreamSslContext();
                        if (sslContext.isEmpty()) {
                            promise.setFailure(new IllegalStateException("Virtual cluster %s does not provide SSL context".formatted(gateway)));
                        }
                        else {
                            KafkaProxyInitializer.this.addHandlers(ch, binding);
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
                    ctx.close();
                }

            }
        });
    }

    @VisibleForTesting
    void addHandlers(Channel ch, EndpointBinding binding) {
        var virtualCluster = binding.endpointGateway().virtualCluster();
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.remove(LOGGING_INBOUND_ERROR_HANDLER_NAME);
        if (virtualCluster.isLogNetwork()) {
            pipeline.addLast("networkLogger", new LoggingHandler("io.kroxylicious.proxy.internal.DownstreamNetworkLogger", LogLevel.INFO));
        }

        ProxyChannelStateMachine proxyChannelStateMachine = new ProxyChannelStateMachine(virtualCluster.getClusterName(), binding.nodeId(), connectionTracker,
                inFlightTracker);

        // TODO https://github.com/kroxylicious/kroxylicious/issues/287 this is in the wrong place, proxy protocol comes over the wire first (so before SSL handler).
        if (haproxyProtocol) {
            LOGGER.debug("Adding haproxy handlers");
            pipeline.addLast("HAProxyMessageDecoder", new HAProxyMessageDecoder());
            // HAProxyMessageHandler intercepts HAProxyMessage and forwards it to the state machine,
            // preventing it from propagating to filters that only expect Kafka protocol messages
            pipeline.addLast("HAProxyMessageHandler", new HAProxyMessageHandler(proxyChannelStateMachine));
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
                filterChainFactoryRef,
                virtualCluster.getFilters(),
                endpointReconciler,
                apiVersionsService,
                dp,
                virtualCluster.subjectBuilder(pfr),
                binding,
                proxyChannelStateMachine, proxyNettySettings,
                connectionDrainManager);

        pipeline.addLast("netHandler", frontendHandler);
        addLoggingErrorHandler(pipeline);

        LOGGER.debug("{}: Initial pipeline: {}", ch, pipeline);
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
                    .log("An exceptionCaught() event was caught by the error handler {}: {}. Increase log level to DEBUG for stacktrace",
                            cause.getClass().getSimpleName(), cause.getMessage());
        }
    }
}
