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
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseEncoder;
import io.kroxylicious.proxy.internal.filter.ApiVersionsDowngradeFilter;
import io.kroxylicious.proxy.internal.filter.ApiVersionsIntersectFilter;
import io.kroxylicious.proxy.internal.filter.BrokerAddressFilter;
import io.kroxylicious.proxy.internal.filter.EagerMetadataLearner;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.net.Endpoint;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointBindingResolver;
import io.kroxylicious.proxy.internal.net.EndpointListener;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.tag.VisibleForTesting;

public class KafkaProxyInitializer extends ChannelInitializer<SocketChannel> {

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
        addLoggingErrorHandler(pipeline);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private void initPlainChannel(SocketChannel ch, ChannelPipeline pipeline, Optional<String> bindingAddress, int targetPort) {
        pipeline.addLast("plainResolver", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) {

                bindingResolver.resolve(Endpoint.createEndpoint(bindingAddress, targetPort, tls), null)
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

    // deep inheritance tree of SniHandler not something we can fix
    @SuppressWarnings({ "OptionalUsedAsFieldOrParameterType", "java:S110" })
    private void initTlsChannel(SocketChannel ch, ChannelPipeline pipeline, Optional<String> bindingAddress, int targetPort) {
        LOGGER.debug("Adding SSL/SNI handler");
        pipeline.addLast("sniResolver", new SniHandler((sniHostname, promise) -> {
            try {
                Endpoint endpoint = Endpoint.createEndpoint(bindingAddress, targetPort, tls);
                var stage = bindingResolver.resolve(endpoint, sniHostname);
                // completes the netty promise when then resolution completes (success/otherwise).
                stage.handle((binding, t) -> {
                    try {
                        if (t != null) {
                            LOGGER.warn("Exception resolving Virtual Cluster Binding for endpoint {} and sniHostname {}: {}", endpoint, sniHostname, t.getMessage());
                            promise.setFailure(t);
                            return null;
                        }
                        var listener = binding.endpointListener();
                        var sslContext = listener.getDownstreamSslContext();
                        if (sslContext.isEmpty()) {
                            promise.setFailure(new IllegalStateException("Virtual cluster %s does not provide SSL context".formatted(listener)));
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
    void addHandlers(SocketChannel ch, EndpointBinding binding) {
        var virtualCluster = binding.endpointListener().virtualCluster();
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
        KafkaRequestDecoder decoder = new KafkaRequestDecoder(dp, virtualCluster.socketFrameMaxSizeBytes(), apiVersionsService);
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

        final NetFilter netFilter = new InitalizerNetFilter(dp,
                ch,
                binding,
                pfr,
                filterChainFactory,
                virtualCluster.getFilters(),
                endpointReconciler,
                new ApiVersionsIntersectFilter(apiVersionsService),
                new ApiVersionsDowngradeFilter(apiVersionsService));
        var frontendHandler = new KafkaProxyFrontendHandler(netFilter, dp, binding.endpointListener());

        pipeline.addLast("netHandler", frontendHandler);
        addLoggingErrorHandler(pipeline);

        LOGGER.debug("{}: Initial pipeline: {}", ch, pipeline);
    }

    private static void addLoggingErrorHandler(ChannelPipeline pipeline) {
        pipeline.addLast(LOGGING_INBOUND_ERROR_HANDLER_NAME, LOGGING_INBOUND_ERROR_HANDLER);
    }

    @VisibleForTesting
    static class InitalizerNetFilter implements NetFilter {

        private final SaslDecodePredicate decodePredicate;
        private final SocketChannel ch;
        private final EndpointListener listener;
        private final EndpointBinding binding;
        private final PluginFactoryRegistry pfr;
        private final FilterChainFactory filterChainFactory;
        private final List<NamedFilterDefinition> filterDefinitions;
        private final EndpointReconciler endpointReconciler;
        private final ApiVersionsIntersectFilter apiVersionsIntersectFilter;
        private final ApiVersionsDowngradeFilter apiVersionsDowngradeFilter;

        InitalizerNetFilter(SaslDecodePredicate decodePredicate,
                            SocketChannel ch,
                            EndpointBinding binding,
                            PluginFactoryRegistry pfr,
                            FilterChainFactory filterChainFactory,
                            List<NamedFilterDefinition> filterDefinitions,
                            EndpointReconciler endpointReconciler,
                            ApiVersionsIntersectFilter apiVersionsIntersectFilter,
                            ApiVersionsDowngradeFilter apiVersionsDowngradeFilter) {
            this.decodePredicate = decodePredicate;
            this.ch = ch;
            this.listener = binding.endpointListener();
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
                    : FilterAndInvoker.build(apiVersionsIntersectFilter);

            NettyFilterContext filterContext = new NettyFilterContext(ch.eventLoop(), pfr);
            List<FilterAndInvoker> filterChain = filterChainFactory.createFilters(filterContext, filterDefinitions);
            List<FilterAndInvoker> brokerAddressFilters = FilterAndInvoker.build(new BrokerAddressFilter(listener, endpointReconciler));
            var filters = new ArrayList<>(apiVersionFilters);
            filters.addAll(FilterAndInvoker.build(apiVersionsDowngradeFilter));
            filters.addAll(filterChain);
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
