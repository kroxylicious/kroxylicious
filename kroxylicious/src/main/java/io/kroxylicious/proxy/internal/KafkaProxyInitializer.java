/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

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
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SniHandler;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseEncoder;
import io.kroxylicious.proxy.internal.filter.UpstreamBrokerAddressCachingNetFilter;

public class KafkaProxyInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyInitializer.class);
    private static final AttributeKey<Object> VIRTUAL_CLUSTER = AttributeKey.newInstance("virtualHost");

    private final boolean haproxyProtocol;
    private final Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> authnHandlers;
    private final boolean requireTls;
    private final VirtualClusterResolver virtualClusterResolver;
    private final Configuration config;

    public KafkaProxyInitializer(Configuration config, boolean requireTls, VirtualClusterResolver virtualClusterResolver, boolean haproxyProtocol,
                                 Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> authnMechanismHandlers) {
        this.haproxyProtocol = haproxyProtocol;
        this.authnHandlers = authnMechanismHandlers != null ? authnMechanismHandlers : Map.of();
        this.requireTls = requireTls;
        this.virtualClusterResolver = virtualClusterResolver;
        this.config = config;
    }

    @Override
    public void initChannel(SocketChannel ch) {

        LOGGER.trace("Connection from {} to my address {}", ch.remoteAddress(), ch.localAddress());

        ChannelPipeline pipeline = ch.pipeline();

        int targetPort = ch.localAddress().getPort();
        if (requireTls) {
            LOGGER.debug("Adding SSL/SNI handler");
            pipeline.addLast(new SniHandler(hostname -> {
                var virtualCluster = virtualClusterResolver.resolve(hostname, targetPort);
                // TODO Error handling.
                var sslContext = virtualCluster.buildSslContext();
                if (sslContext.isEmpty()) {
                    throw new IllegalStateException("Virtual cluster %s does not provide SSL context".formatted(virtualCluster));
                }
                ch.attr(VIRTUAL_CLUSTER).set(virtualCluster);
                return sslContext.get();
            }));

            // Intercept the SniCompletionEvent and use that as a trigger to add the remainder of the pipeline.
            pipeline.addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                    // The order is important here, we need to add the handlers before propagating the user event.
                    // This is important as our pipeline line needs to hear the SNI event.
                    if (evt instanceof SniCompletionEvent) {
                        var virtualCluster = ((VirtualCluster) ctx.channel().attr(VIRTUAL_CLUSTER).get());
                        addHandlers(ch, virtualCluster);
                        ctx.fireChannelActive();
                        ctx.pipeline().remove(this);
                        ctx.channel().attr(VIRTUAL_CLUSTER).set(null);
                    }
                    super.userEventTriggered(ctx, evt);
                }
            });
        }
        else {
            var virtualCluster = virtualClusterResolver.resolve(targetPort);
            addHandlers(ch, virtualCluster);
        }
    }

    private void addHandlers(SocketChannel ch, VirtualCluster virtualCluster) {
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

        var netFilter = new UpstreamBrokerAddressCachingNetFilter(config, virtualCluster);

        pipeline.addLast("netHandler", new KafkaProxyFrontendHandler(netFilter, dp, virtualCluster.isLogNetwork(), virtualCluster.isLogFrames()));
        LOGGER.debug("{}: Initial pipeline: {}", ch, pipeline);
    }

}
