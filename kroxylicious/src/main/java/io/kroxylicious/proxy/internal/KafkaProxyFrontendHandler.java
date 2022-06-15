/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kroxylicious.proxy.internal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LoggingHandler;

public class KafkaProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LogManager.getLogger(KafkaProxyFrontendHandler.class);

    private final String remoteHost;
    private final int remotePort;
    private final CorrelationManager correlationManager;
    private final boolean logNetwork;
    private final boolean logFrames;
    private final KrpcFilter[] filters;

    private ChannelHandlerContext outboundCtx;
    private KafkaProxyBackendHandler backendHandler;
    private boolean pendingFlushes;
    private ChannelHandlerContext blockedInboundCtx;

    public KafkaProxyFrontendHandler(String remoteHost,
                                     int remotePort,
                                     CorrelationManager correlationManager,
                                     KrpcFilter[] filters,
                                     boolean logNetwork,
                                     boolean logFrames) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.correlationManager = correlationManager;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.filters = filters;
    }

    public void outboundChannelActive(ChannelHandlerContext ctx) {
        outboundCtx = ctx;
    }

    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        // this is key to propagate back-pressure changes
        if (backendHandler != null) {
            backendHandler.inboundChannelWritabilityChanged(ctx);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        LOGGER.trace("Channel active {}", ctx);
        final Channel inboundChannel = ctx.channel();

        // Start the upstream connection attempt.
        Bootstrap b = new Bootstrap();
        backendHandler = new KafkaProxyBackendHandler(this, ctx);
        b.group(inboundChannel.eventLoop())
                .channel(ctx.channel().getClass())
                .handler(backendHandler)
                .option(ChannelOption.AUTO_READ, true)
                .option(ChannelOption.TCP_NODELAY, true);

        LOGGER.trace("Connecting to outbound {}:{}", remoteHost, remotePort);
        ChannelFuture connectFuture = b.connect(remoteHost, remotePort);
        Channel outboundChannel = connectFuture.channel();
        ChannelPipeline pipeline = outboundChannel.pipeline();

        if (logFrames) {
            pipeline.addFirst("frameLogger", new LoggingHandler("backend-application"));
        }
        addFiltersToPipeline(pipeline);
        pipeline.addFirst("responseDecoder", new KafkaResponseDecoder(correlationManager));
        pipeline.addFirst("requestEncoder", new KafkaRequestEncoder(correlationManager));
        if (logNetwork) {
            pipeline.addFirst("networkLogger", new LoggingHandler("backend-network"));
        }

        connectFuture.addListener(future -> {
            if (future.isSuccess()) {
                LOGGER.trace("Outbound connect complete ({}), register interest to read on inbound channel {}", outboundChannel.localAddress(), inboundChannel);
                // connection complete start to read first data
                inboundChannel.config().setAutoRead(true);
            }
            else {
                // Close the connection if the connection attempt has failed.
                LOGGER.trace("Outbound connect error, closing inbound channel", future.cause());
                inboundChannel.close();
            }
        });
    }

    private void addFiltersToPipeline(ChannelPipeline pipeline) {
        for (var filter : filters) {
            pipeline.addFirst(filter.toString(), new FilterHandler(filter));
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        LOGGER.trace("Completed read on inbound channel: {}", msg);
        if (outboundCtx == null) {
            LOGGER.trace("Outbound is not active");
            return;
        }
        final Channel outboundChannel = outboundCtx.channel();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Outbound writable: {}", outboundChannel.isWritable());
            LOGGER.trace("Outbound bytesBeforeUnwritable: {}", outboundChannel.bytesBeforeUnwritable());
            LOGGER.trace("Outbound config: {}", outboundChannel.config());
            LOGGER.trace("Outbound is active, writing and flushing {}", msg);
        }
        if (outboundChannel.isWritable()) {
            outboundChannel.write(msg, outboundCtx.voidPromise());
            pendingFlushes = true;
        }
        else {
            outboundChannel.writeAndFlush(msg, outboundCtx.voidPromise());
            pendingFlushes = false;
        }
    }

    public void outboundWritabilityChanged(ChannelHandlerContext outboundCtx) {
        assert this.outboundCtx == outboundCtx;
        final ChannelHandlerContext inboundCtx = blockedInboundCtx;
        if (inboundCtx != null && outboundCtx.channel().isWritable()) {
            blockedInboundCtx = null;
            inboundCtx.channel().config().setAutoRead(true);
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        if (outboundCtx == null) {
            LOGGER.trace("Outbound is not active");
            return;
        }
        final Channel outboundChannel = outboundCtx.channel();
        if (pendingFlushes) {
            pendingFlushes = false;
            outboundChannel.flush();
        }
        if (!outboundChannel.isWritable()) {
            ctx.channel().config().setAutoRead(false);
            this.blockedInboundCtx = ctx;
        }

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (outboundCtx == null) {
            return;
        }
        final Channel outboundChannel = outboundCtx.channel();
        if (outboundChannel != null) {
            closeOnFlush(outboundChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        closeOnFlush(ctx.channel());
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

}
