/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.ArrayDeque;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * This handler is installed directly after an SSLHandler. The intent is to buffer all channelRead data
 * until a signal has been received that the Transport Subject has been built, at which point it will
 * fire all its buffered objects into the channel and remove itself from the pipeline.
 * This catches edge cases where messages traverse the Filter chain before the Transport Subject has been
 * asynchronously computed.
 */
class BufferingGatewayHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(BufferingGatewayHandler.class);
    private boolean gateOpen = false;
    private final Queue<Object> buffer = new ArrayDeque<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (gateOpen) {
            ctx.fireChannelRead(msg);
        }
        else {
            LOGGER.atDebug()
                    .addKeyValue("channelId", ctx.channel().id())
                    .log("buffering event");
            buffer.add(msg);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof TransportSubjectBuilt) {
            LOGGER.atDebug()
                    .addArgument(buffer.size())
                    .addKeyValue("channelId", ctx.channel().id())
                    .log("Transport Subject Built, firing channel reads for {} buffered events");
            gateOpen = true;
            while (!buffer.isEmpty()) {
                ctx.fireChannelRead(buffer.poll());
            }
            ctx.pipeline().remove(this);
            ctx.read();
        }
        ctx.fireUserEventTriggered(evt);
    }
}
