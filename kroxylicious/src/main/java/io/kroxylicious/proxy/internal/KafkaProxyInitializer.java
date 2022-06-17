/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.kroxylicious.proxy.filter.FilterChainFactory;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class KafkaProxyInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger LOGGER = LogManager.getLogger(KafkaProxyInitializer.class);

    private final String remoteHost;
    private final int remotePort;
    private final FilterChainFactory filterChainFactory;
    private final boolean logNetwork;
    private final boolean logFrames;

    public KafkaProxyInitializer(String remoteHost,
                                 int remotePort,
                                 FilterChainFactory filterChainFactory,
                                 boolean logNetwork,
                                 boolean logFrames) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.filterChainFactory = filterChainFactory;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        // TODO TLS

        LOGGER.trace("Connection from {} to my address {}", ch.remoteAddress(), ch.localAddress());

        var correlation = new CorrelationManager();

        ChannelPipeline pipeline = ch.pipeline();
        if (logNetwork) {
            pipeline.addLast("networkLogger", new LoggingHandler("frontend-network", LogLevel.INFO));
        }
        var filters = filterChainFactory.createFilters();
        // The decoder, this only cares about the filters
        // because it needs to know whether to decode requests
        KafkaRequestDecoder decoder = new KafkaRequestDecoder(filters);
        pipeline.addLast("requestDecoder", decoder);

        pipeline.addLast("responseEncoder", new KafkaResponseEncoder());
        if (logFrames) {
            pipeline.addLast("frameLogger", new LoggingHandler("frontend-application", LogLevel.INFO));
        }

        pipeline.addLast("frontendHandler", new KafkaProxyFrontendHandler(remoteHost,
                remotePort,
                correlation,
                filters,
                logNetwork,
                logFrames));
    }

}
