/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
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
import io.netty.util.concurrent.Future;

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.internal.codec.KafkaMessageListener;
import io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseEncoder;
import io.kroxylicious.proxy.internal.filter.ApiVersionsDowngradeFilter;
import io.kroxylicious.proxy.internal.filter.ApiVersionsIntersectFilter;
import io.kroxylicious.proxy.internal.filter.BrokerAddressFilter;
import io.kroxylicious.proxy.internal.filter.EagerMetadataLearner;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.metrics.MetricEmittingKafkaMessageListener;
import io.kroxylicious.proxy.internal.net.Endpoint;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointBindingResolver;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.tag.VisibleForTesting;

public class KafkaProxyInitializer extends ChannelInitializer<Channel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyInitializer.class);

    private static final ChannelInboundHandlerAdapter LOGGING_INBOUND_ERROR_HANDLER = new LoggingInboundErrorHandler();
    @VisibleForTesting
    static final String LOGGING_INBOUND_ERROR_HANDLER_NAME = "loggingInboundErrorHandler";

    private final boolean haproxyProtocol;
    private final Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> authnHandlers;
    private final boolean tls;
    private final EndpointBindingResolver bindingResolver;
    private final EndpointReconciler endpointReconciler;
    private final PluginFactoryRegistry pfr;
    private final FilterChainFactory filterChainFactory;
    private final ApiVersionsServiceImpl apiVersionsService;
    private final Counter clientToProxyErrorCounter;

    public KafkaProxyInitializer(FilterChainFactory filterChainFactory,
                                 PluginFactoryRegistry pfr,
                                 boolean tls,
                                 EndpointBindingResolver bindingResolver,
                                 EndpointReconciler endpointReconciler,
                                 boolean haproxyProtocol,
                                 Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> authnMechanismHandlers,
                                 ApiVersionsServiceImpl apiVersionsService) {
        this.pfr = pfr;
        this.endpointReconciler = endpointReconciler;
        this.haproxyProtocol = haproxyProtocol;
        this.authnHandlers = authnMechanismHandlers != null ? authnMechanismHandlers : Map.of();
        this.tls = tls;
        this.bindingResolver = bindingResolver;
        this.filterChainFactory = filterChainFactory;
        this.apiVersionsService = apiVersionsService;
        this.clientToProxyErrorCounter = Metrics.clientToProxyErrorCounter("", null).withTags();
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
        addLoggingErrorHandler(ch.pipeline());
    }

    private void initPlainChannel(Channel ch) {
        ch.pipeline().addLast("plainResolver", new ChannelInboundHandlerAdapter() {
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

    // deep inheritance tree of SniHandler not something we can fix
    @SuppressWarnings({ "java:S110" })
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

        // Add handler here
        // TODO https://github.com/kroxylicious/kroxylicious/issues/287 this is in the wrong place, proxy protocol comes over the wire first (so before SSL handler).
        if (haproxyProtocol) {
            LOGGER.debug("Adding haproxy handler");
            pipeline.addLast("HAProxyMessageDecoder", new HAProxyMessageDecoder());
        }

        var dp = new SaslDecodePredicate(!authnHandlers.isEmpty());
        // The decoder, this only cares about the filters
        // because it needs to know whether to decode requests

        var encoderListener = buildMetricsMessageListenerForEncode(binding, virtualCluster);
        var decoderListener = buildMetricsMessageListenerForDecode(binding, virtualCluster);

        KafkaRequestDecoder decoder = new KafkaRequestDecoder(dp, virtualCluster.socketFrameMaxSizeBytes(), apiVersionsService, decoderListener);
        pipeline.addLast("requestDecoder", decoder);
        pipeline.addLast("responseEncoder", new KafkaResponseEncoder(encoderListener));
        pipeline.addLast("responseOrderer", new ResponseOrderer());
        if (virtualCluster.isLogFrames()) {
            pipeline.addLast("frameLogger", new LoggingHandler("io.kroxylicious.proxy.internal.DownstreamFrameLogger", LogLevel.INFO));
        }

        if (!authnHandlers.isEmpty()) {
            LOGGER.debug("Adding authn handler for handlers {}", authnHandlers);
            pipeline.addLast(new KafkaAuthnHandler(ch, authnHandlers));
        }

        final NetFilter netFilter = new InitalizerNetFilter(dp,
                ch,
                binding,
                pfr,
                filterChainFactory,
                virtualCluster.getFilters(),
                endpointReconciler,
                new ApiVersionsIntersectFilter(apiVersionsService),
                new ApiVersionsDowngradeFilter(apiVersionsService));
        var frontendHandler = new KafkaProxyFrontendHandler(netFilter, dp, binding, new ProxyChannelStateMachine(virtualCluster.getClusterName(), binding.nodeId()));

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

    @VisibleForTesting
    static class InitalizerNetFilter implements NetFilter {

        private final SaslDecodePredicate decodePredicate;
        private final Channel ch;
        private final EndpointGateway gateway;
        private final EndpointBinding binding;
        private final PluginFactoryRegistry pfr;
        private final FilterChainFactory filterChainFactory;
        private final List<NamedFilterDefinition> filterDefinitions;
        private final EndpointReconciler endpointReconciler;
        private final ApiVersionsIntersectFilter apiVersionsIntersectFilter;
        private final ApiVersionsDowngradeFilter apiVersionsDowngradeFilter;

        InitalizerNetFilter(SaslDecodePredicate decodePredicate,
                            Channel ch,
                            EndpointBinding binding,
                            PluginFactoryRegistry pfr,
                            FilterChainFactory filterChainFactory,
                            List<NamedFilterDefinition> filterDefinitions,
                            EndpointReconciler endpointReconciler,
                            ApiVersionsIntersectFilter apiVersionsIntersectFilter,
                            ApiVersionsDowngradeFilter apiVersionsDowngradeFilter) {
            this.decodePredicate = decodePredicate;
            this.ch = ch;
            this.gateway = binding.endpointGateway();
            this.binding = binding;
            this.pfr = pfr;
            this.filterChainFactory = filterChainFactory;
            this.filterDefinitions = filterDefinitions;
            this.endpointReconciler = endpointReconciler;
            this.apiVersionsIntersectFilter = apiVersionsIntersectFilter;
            this.apiVersionsDowngradeFilter = apiVersionsDowngradeFilter;
        }

        @Override
        public void selectServer(NetFilter.NetFilterContext context) {
            List<FilterAndInvoker> apiVersionFilters = decodePredicate.isAuthenticationOffloadEnabled() ? List.of()
                    : FilterAndInvoker.build("ApiVersionsIntersect (internal)", apiVersionsIntersectFilter);

            NettyFilterContext filterContext = new NettyFilterContext(ch.eventLoop(), pfr);
            List<FilterAndInvoker> filterChain = filterChainFactory.createFilters(filterContext, filterDefinitions);
            List<FilterAndInvoker> brokerAddressFilters = FilterAndInvoker.build("BrokerAddress (internal)", new BrokerAddressFilter(gateway, endpointReconciler));
            var filters = new ArrayList<>(apiVersionFilters);
            filters.addAll(FilterAndInvoker.build("ApiVersionsDowngrade (internal)", apiVersionsDowngradeFilter));
            filters.addAll(filterChain);
            if (binding.restrictUpstreamToMetadataDiscovery()) {
                filters.addAll(FilterAndInvoker.build("EagerMetadataLearner (internal)", new EagerMetadataLearner()));
            }
            filters.addAll(brokerAddressFilters);

            var target = binding.upstreamTarget();
            if (target == null) {
                // This condition should never happen.
                throw new IllegalStateException("A target address for binding %s is not known.".formatted(binding));
            }

            context.initiateConnect(target, filters);
        }
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
