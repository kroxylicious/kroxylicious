/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

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
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseEncoder;
import io.kroxylicious.proxy.internal.net.EndpointResolver;
import io.kroxylicious.proxy.internal.net.VirtualClusterBinding;
import io.kroxylicious.proxy.internal.net.VirtualClusterBrokerBinding;
import io.kroxylicious.proxy.service.HostPort;

public class KafkaProxyInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyInitializer.class);

    private final boolean haproxyProtocol;
    private final Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> authnHandlers;
    private final boolean tls;
    private final EndpointResolver endpointResolver;
    private final Configuration config;

    public KafkaProxyInitializer(Configuration config, boolean tls, EndpointResolver endpointResolver, boolean haproxyProtocol,
                                 Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> authnMechanismHandlers) {
        this.haproxyProtocol = haproxyProtocol;
        this.authnHandlers = authnMechanismHandlers != null ? authnMechanismHandlers : Map.of();
        this.tls = tls;
        this.endpointResolver = endpointResolver;
        this.config = config;
    }

    @Override
    public void initChannel(SocketChannel ch) {

        LOGGER.trace("Connection from {} to my address {}", ch.remoteAddress(), ch.localAddress());

        ChannelPipeline pipeline = ch.pipeline();

        int targetPort = ch.localAddress().getPort();
        var bindingAddress = ch.parent().localAddress().getAddress().isAnyLocalAddress() ? null : ch.localAddress().getAddress().getHostAddress();
        if (tls) {
            LOGGER.debug("Adding SSL/SNI handler");
            pipeline.addLast(new SniHandler((sniHostname, promise) -> {
                try {
                    var stage = endpointResolver.resolve(bindingAddress, targetPort, sniHostname, tls);
                    // completes the netty promise when then resolution completes (success/otherwise).
                    var unused = stage.handle((binding, t) -> {
                        try {
                            if (t != null) {
                                promise.setFailure(t);
                                return null;
                            }
                            var virtualCluster = binding.virtualCluster();
                            var sslContext = virtualCluster.buildSslContext();
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
        else {
            pipeline.addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelActive(ChannelHandlerContext ctx) {
                    var stage = endpointResolver.resolve(bindingAddress, targetPort, null, tls);
                    var unused = stage.handle((binding, t) -> {
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
    }

    private void addHandlers(SocketChannel ch, VirtualClusterBinding binding) {
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
        KafkaRequestDecoder decoder = new KafkaRequestDecoder(dp);
        pipeline.addLast("requestDecoder", decoder);

        pipeline.addLast("responseEncoder", new KafkaResponseEncoder());
        if (virtualCluster.isLogFrames()) {
            pipeline.addLast("frameLogger", new LoggingHandler("io.kroxylicious.proxy.internal.DownstreamFrameLogger", LogLevel.INFO));
        }

        if (!authnHandlers.isEmpty()) {
            LOGGER.debug("Adding authn handler for handlers {}", authnHandlers);
            pipeline.addLast(new KafkaAuthnHandler(ch, authnHandlers));
        }

        var frontendHandler = new KafkaProxyFrontendHandler(context -> {
            var filterChainFactory = new FilterChainFactory(config, virtualCluster.getClusterEndpointProvider());

            var filters = new ArrayList<>(Arrays.stream(filterChainFactory.createFilters()).toList());

            // Add a filter to the *end of the chain* that gathers the true nodeId/upstream broker mapping.
            filters.add((MetadataResponseFilter) (header, response, filterContext) -> {
                response.brokers().forEach(b -> {
                    var replacement = new HostPort(b.host(), b.port());
                    var existing = virtualCluster.updateUpstreamClusterAddressForNode(b.nodeId(), replacement);
                    if (!replacement.equals(existing)) {
                        LOGGER.info("Got upstream for broker {} : {}", b.nodeId(), replacement);
                    }
                });
                filterContext.forwardResponse(response);
            });

            var targetBootstrapServers = virtualCluster.targetCluster().bootstrapServers();
            var targetBootstrapServersParts = targetBootstrapServers.split(",");
            var targetClusterBootstrap = HostPort.parse(targetBootstrapServersParts[0]);

            HostPort target;
            if (binding instanceof VirtualClusterBrokerBinding brokerBinding) {
                var upstreamBroker = virtualCluster.getUpstreamClusterAddressForNode(brokerBinding.nodeId());
                if (upstreamBroker != null) {
                    target = upstreamBroker;
                }
                else {
                    // TODO: this behaviour is sub-optimal as it means a client will proceed with a connection to the wrong broker.
                    // This will lead to difficult to diagnose failure cases later (produces going to the wrong broker, metadata refresh cycles, etc).
                    LOGGER.warn("An upstream address for broker {} is not yet known, connecting the client to bootstrap instead.", brokerBinding.nodeId());
                    target = targetClusterBootstrap;
                }
            }
            else {
                target = targetClusterBootstrap;
            }

            context.initiateConnect(target.host(), target.port(), filters.toArray(new KrpcFilter[0]));
        }, dp, virtualCluster.isLogNetwork(), virtualCluster.isLogFrames());

        pipeline.addLast("netHandler", frontendHandler);

        LOGGER.debug("{}: Initial pipeline: {}", ch, pipeline);
    }

}
