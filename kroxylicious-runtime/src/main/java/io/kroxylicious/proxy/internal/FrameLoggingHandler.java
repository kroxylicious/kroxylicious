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
import io.netty.util.internal.logging.InternalLogger;

import io.kroxylicious.proxy.tag.VisibleForTesting;

public class FrameLoggingHandler extends LoggingHandler {
    private final InternalLogLevel frameLevel;
    private InternalLogger nettyLogger;

    public FrameLoggingHandler(String name, LogLevel frameLevel) {
        super(name);
        this.frameLevel = frameLevel.toInternalLevel();
        this.nettyLogger = logger;
    }

    @VisibleForTesting
    FrameLoggingHandler(String name, LogLevel frameLevel, InternalLogger nettyLogger) {
        super(name);
        this.frameLevel = frameLevel.toInternalLevel();
        this.nettyLogger = nettyLogger;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (nettyLogger.isEnabled(frameLevel)) {
            nettyLogger.log(frameLevel, format(ctx, "READ", msg));
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (nettyLogger.isEnabled(frameLevel)) {
            nettyLogger.log(frameLevel, format(ctx, "WRITE", msg));
        }
        ctx.write(msg, promise);
    }

}
