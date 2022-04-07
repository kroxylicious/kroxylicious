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
package io.strimzi.kproxy;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class KafkaProxyBackendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LogManager.getLogger(KafkaProxyBackendHandler.class);

    private final ChannelHandlerContext inboundCtx;
    private ChannelHandlerContext blockedOutboundCtx;
    private boolean unflushedWrites;

    public KafkaProxyBackendHandler(ChannelHandlerContext inboundCtx) {
        this.inboundCtx = requireNonNull(inboundCtx);
    }

    public void inboundChannelWritabilityChanged(ChannelHandlerContext inboundCtx) {
        assert inboundCtx == this.inboundCtx;
        final ChannelHandlerContext outboundCtx = blockedOutboundCtx;
        if (outboundCtx != null && inboundCtx.channel().isWritable()) {
            blockedOutboundCtx = null;
            outboundCtx.channel().config().setAutoRead(true);
        }
    }

    // Called when the outbound channel is active
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.trace("Channel active {}", ctx);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        assert blockedOutboundCtx == null;
        LOGGER.trace(msg);
        final Channel inboundChannel = inboundCtx.channel();
        if (inboundChannel.isWritable()) {
            inboundChannel.write(msg, inboundCtx.voidPromise());
            unflushedWrites = true;
        } else {
            inboundChannel.writeAndFlush(msg, inboundCtx.voidPromise());
            unflushedWrites = false;
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        final Channel inboundChannel = inboundCtx.channel();
        if (unflushedWrites) {
            unflushedWrites = false;
            inboundChannel.flush();
        }
        if (!inboundChannel.isWritable()) {
            ctx.channel().config().setAutoRead(false);
            this.blockedOutboundCtx = ctx;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        KafkaProxyFrontendHandler.closeOnFlush(inboundCtx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        KafkaProxyFrontendHandler.closeOnFlush(ctx.channel());
    }
}
