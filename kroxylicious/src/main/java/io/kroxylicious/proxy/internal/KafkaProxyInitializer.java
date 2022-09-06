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

import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class KafkaProxyInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyInitializer.class);

    private final boolean logNetwork;
    private final boolean logFrames;
    private final boolean haproxyProtocol;
    private final Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> authnHandlers;
    private final NetFilter netFilter;

    public KafkaProxyInitializer(boolean haproxyProtocol,
                                 Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> authnMechanismHandlers,
                                 NetFilter netFilter,
                                 boolean logNetwork,
                                 boolean logFrames) {
        this.haproxyProtocol = haproxyProtocol;
        this.authnHandlers = authnMechanismHandlers;
        this.netFilter = netFilter;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        // TODO TLS

        LOGGER.trace("Connection from {} to my address {}", ch.remoteAddress(), ch.localAddress());

        ChannelPipeline pipeline = ch.pipeline();
        if (logNetwork) {
            pipeline.addLast("networkLogger", new LoggingHandler("io.kroxylicious.proxy.internal.DownstreamNetworkLogger", LogLevel.INFO));
        }

        // Add handler here
        if (haproxyProtocol) {
            LOGGER.debug("Adding haproxy handler");
            pipeline.addLast("HAProxyMessageDecoder", new HAProxyMessageDecoder());
        }

        var dp = new MyDecodePredicate(authnHandlers != null && !authnHandlers.isEmpty());
        // The decoder, this only cares about the filters
        // because it needs to know whether to decode requests
        KafkaRequestDecoder decoder = new KafkaRequestDecoder(dp);
        pipeline.addLast("requestDecoder", decoder);

        pipeline.addLast("responseEncoder", new KafkaResponseEncoder());
        if (logFrames) {
            pipeline.addLast("frameLogger", new LoggingHandler("io.kroxylicious.proxy.internal.DownstreamFrameLogger", LogLevel.INFO));
        }

        if (!authnHandlers.isEmpty()) {
            LOGGER.debug("Adding authn handler for handlers {}", authnHandlers);
            pipeline.addLast(new KafkaAuthnHandler(authnHandlers));
        }

        pipeline.addLast("netHandler", new KafkaProxyFrontendHandler(netFilter, dp, logNetwork, logFrames));
        LOGGER.debug("{}: Initial pipeline: {}", ch, pipeline);
    }

}
