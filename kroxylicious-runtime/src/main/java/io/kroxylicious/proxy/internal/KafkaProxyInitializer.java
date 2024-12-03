/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SniHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseEncoder;
import io.kroxylicious.proxy.internal.filter.ApiVersionsIntersectFilter;
import io.kroxylicious.proxy.internal.filter.BrokerAddressFilter;
import io.kroxylicious.proxy.internal.filter.EagerMetadataLearner;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.net.Endpoint;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.net.VirtualClusterBinding;
import io.kroxylicious.proxy.internal.net.VirtualClusterBindingResolver;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.tag.VisibleForTesting;

public class KafkaProxyInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyInitializer.class);

    private final boolean haproxyProtocol;
    private final Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> authnHandlers;
    private final boolean tls;
    private final VirtualClusterBindingResolver virtualClusterBindingResolver;
    private final EndpointReconciler endpointReconciler;
    private final PluginFactoryRegistry pfr;
    private final FilterChainFactory filterChainFactory;
    private final ApiVersionsServiceImpl apiVersionsService;

    public KafkaProxyInitializer(FilterChainFactory filterChainFactory,
                                 PluginFactoryRegistry pfr,
                                 boolean tls,
                                 VirtualClusterBindingResolver virtualClusterBindingResolver,
                                 EndpointReconciler endpointReconciler,
                                 boolean haproxyProtocol,
                                 Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> authnMechanismHandlers,
                                 ApiVersionsServiceImpl apiVersionsService) {
        this.pfr = pfr;
        this.endpointReconciler = endpointReconciler;
        this.haproxyProtocol = haproxyProtocol;
        this.authnHandlers = authnMechanismHandlers != null ? authnMechanismHandlers : Map.of();
        this.tls = tls;
        this.virtualClusterBindingResolver = virtualClusterBindingResolver;
        this.filterChainFactory = filterChainFactory;
        this.apiVersionsService = apiVersionsService;
    }

    @Override
    public void initChannel(SocketChannel ch) {

        LOGGER.trace("Connection from {} to my address {}", ch.remoteAddress(), ch.localAddress());

        ChannelPipeline pipeline = ch.pipeline();

        int targetPort = ch.localAddress().getPort();
        var bindingAddress = ch.parent().localAddress().getAddress().isAnyLocalAddress() ? Optional.<String> empty()
                : Optional.of(ch.localAddress().getAddress().getHostAddress());
        if (tls) {
            initTlsChannel(ch, pipeline, bindingAddress, targetPort);
        }
        else {
            initPlainChannel(ch, pipeline, bindingAddress, targetPort);
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private void initPlainChannel(SocketChannel ch, ChannelPipeline pipeline, Optional<String> bindingAddress, int targetPort) {
        pipeline.addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) {
                virtualClusterBindingResolver.resolve(Endpoint.createEndpoint(bindingAddress, targetPort, tls), null)
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
                                pipeline.remove(this);
                            }
                            return null;
                        });
            }
        });
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private void initTlsChannel(SocketChannel ch, ChannelPipeline pipeline, Optional<String> bindingAddress, int targetPort) {
        LOGGER.debug("Adding SSL/SNI handler");
        pipeline.addLast(new SniHandler((sniHostname, promise) -> {
            try {
                var stage = virtualClusterBindingResolver.resolve(Endpoint.createEndpoint(bindingAddress, targetPort, tls), sniHostname);
                // completes the netty promise when then resolution completes (success/otherwise).
                stage.handle((binding, t) -> {
                    try {
                        if (t != null) {
                            promise.setFailure(t);
                            return null;
                        }
                        var virtualCluster = binding.virtualCluster();
                        var sslContext = virtualCluster.getDownstreamSslContext();
                        if (sslContext.isEmpty()) {
                            promise.setFailure(new IllegalStateException("Virtual cluster %s does not provide SSL context".formatted(virtualCluster)));
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
                super.onLookupComplete(ctx, future);
                ctx.fireChannelActive();
            }
        });
    }

    @VisibleForTesting
    void addHandlers(SocketChannel ch, VirtualClusterBinding binding) {
        var virtualCluster = binding.virtualCluster();
        ChannelPipeline pipeline = ch.pipeline();
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
        KafkaRequestDecoder decoder = new KafkaRequestDecoder(dp, virtualCluster.socketFrameMaxSizeBytes());
        pipeline.addLast("requestDecoder", decoder);
        pipeline.addLast("responseEncoder", new KafkaResponseEncoder());
        pipeline.addLast("responseOrderer", new ResponseOrderer());
        if (virtualCluster.isLogFrames()) {
            pipeline.addLast("frameLogger", new LoggingHandler("io.kroxylicious.proxy.internal.DownstreamFrameLogger", LogLevel.INFO));
        }

        if (!authnHandlers.isEmpty()) {
            LOGGER.debug("Adding authn handler for handlers {}", authnHandlers);
            pipeline.addLast(new KafkaAuthnHandler(ch, authnHandlers));
        }

        final NetFilter netFilter = new InitalizerNetFilter(dp, ch, binding, pfr, filterChainFactory, endpointReconciler,
                new ApiVersionsIntersectFilter(apiVersionsService));
        var frontendHandler = new KafkaProxyFrontendHandler(netFilter, dp, virtualCluster);

        pipeline.addLast("netHandler", frontendHandler);

        LOGGER.debug("{}: Initial pipeline: {}", ch, pipeline);
    }

    @VisibleForTesting
    static class InitalizerNetFilter implements NetFilter {

        private final SaslDecodePredicate decodePredicate;
        private final SocketChannel ch;
        private final VirtualCluster virtualCluster;
        private final VirtualClusterBinding binding;
        private final PluginFactoryRegistry pfr;
        private final FilterChainFactory filterChainFactory;
        private final EndpointReconciler endpointReconciler;
        private final ApiVersionsIntersectFilter apiVersionsIntersectFilter;

        InitalizerNetFilter(SaslDecodePredicate decodePredicate,
                            SocketChannel ch,
                            VirtualClusterBinding binding,
                            PluginFactoryRegistry pfr,
                            FilterChainFactory filterChainFactory,
                            EndpointReconciler endpointReconciler,
                            ApiVersionsIntersectFilter apiVersionsIntersectFilter) {
            this.decodePredicate = decodePredicate;
            this.ch = ch;
            this.virtualCluster = binding.virtualCluster();
            this.binding = binding;
            this.pfr = pfr;
            this.filterChainFactory = filterChainFactory;
            this.endpointReconciler = endpointReconciler;
            this.apiVersionsIntersectFilter = apiVersionsIntersectFilter;
        }

        @Override
        public void selectServer(NetFilter.NetFilterContext context) {
            List<FilterAndInvoker> apiVersionFilters = decodePredicate.isAuthenticationOffloadEnabled() ? List.of()
                    : FilterAndInvoker.build(apiVersionsIntersectFilter);

            NettyFilterContext filterContext = new NettyFilterContext(ch.eventLoop(), pfr);
            List<FilterAndInvoker> customProtocolFilters = filterChainFactory.createFilters(filterContext);
            List<FilterAndInvoker> brokerAddressFilters = FilterAndInvoker.build(new BrokerAddressFilter(virtualCluster, endpointReconciler));
            var filters = new ArrayList<>(apiVersionFilters);
            filters.addAll(customProtocolFilters);
            if (binding.restrictUpstreamToMetadataDiscovery()) {
                filters.addAll(FilterAndInvoker.build(new EagerMetadataLearner()));
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
}
