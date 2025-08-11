/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.logging.InternalLogLevel;

public class FameLoggingHandler extends LoggingHandler {
    private final InternalLogLevel frameLevel;

    public FameLoggingHandler(String name, LogLevel frameLevel) {
        super(name);
        this.frameLevel = frameLevel.toInternalLevel();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (logger.isEnabled(frameLevel)) {
            logger.log(frameLevel, format(ctx, "READ", msg));
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (logger.isEnabled(frameLevel)) {
            logger.log(frameLevel, format(ctx, "WRITE", msg));
        }
        ctx.write(msg, promise);
    }

}
