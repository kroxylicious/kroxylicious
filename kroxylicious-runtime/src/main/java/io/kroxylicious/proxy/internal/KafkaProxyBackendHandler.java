/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.ssl.SslContext;

import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Optional;

public class KafkaProxyBackendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyBackendHandler.class);

    @VisibleForTesting final StateHolder stateHolder;
    @VisibleForTesting final SslContext sslContext;
    private ChannelHandlerContext serverCtx;
    private boolean pendingServerFlushes;

    public KafkaProxyBackendHandler(
            StateHolder stateHolder,
            VirtualCluster virtualCluster) {
        this.stateHolder = stateHolder;
        Optional<SslContext> upstreamSslContext = virtualCluster.getUpstreamSslContext();
        this.sslContext = upstreamSslContext.orElse(null);
    }

    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        // TODO you're here, and you need to change this to be in terms of
        // a stateHolder field
        // i.e. stateHolder.onServerBlocked/onServerUnblocked
        //frontendHandler.upstreamWritabilityChanged(ctx);
        if (ctx.channel().isWritable()) {
            stateHolder.onServerWritable();
        }
        else {
            stateHolder.onServerUnwritable();
        }
    }

    // Called when the outbound channel is active
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.trace("Channel active {}", ctx);
        serverCtx = ctx;
        stateHolder.onServerActive(ctx, sslContext);
        super.channelActive(ctx);
    }

    @Override
    public void userEventTriggered(
            ChannelHandlerContext ctx,
            Object evt
    ) throws Exception {
        if (evt instanceof SslHandshakeCompletionEvent sslEvt) {
            stateHolder.onServerTlsHandshakeCompletion(sslEvt);
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        stateHolder.onServerInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        stateHolder.onServerException(cause);
    }

    /**
     * Relieve backpressure on the server connection by turning on auto-read.
     */
    public void inboundChannelWritabilityChanged() {
        if (serverCtx != null) {
            serverCtx.channel().config().setAutoRead(true);
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        stateHolder.forwardToClient(msg);
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        stateHolder.serverReadComplete();
    }

    public void forwardToServer(Object msg) {
        if (serverCtx == null) {
            LOGGER.trace("WRITE to server ignored because outbound is not active (msg: {})", msg);
            return;
        }
        final Channel outboundChannel = serverCtx.channel();
        if (outboundChannel.isWritable()) {
            outboundChannel.write(msg, serverCtx.voidPromise());
            pendingServerFlushes = true;
        }
        else {
            outboundChannel.writeAndFlush(msg, serverCtx.voidPromise());
            pendingServerFlushes = false;
        }
        LOGGER.trace("/READ");
    }

    public void flushToServer() {
        final Channel serverChannel = serverCtx.channel();
        if (pendingServerFlushes) {
            pendingServerFlushes = false;
            serverChannel.flush();
        }
        if (!serverChannel.isWritable()) {
            stateHolder.onServerUnwritable();
        }
    }

    public void blockServerReads() {
        if (serverCtx != null) {
            serverCtx.channel().config().setAutoRead(false);
        }
    }

    public void unblockServerReads() {
        if (serverCtx != null) {
            serverCtx.channel().config().setAutoRead(true);
        }
    }

    public void close() {
        if (serverCtx != null) {
            Channel outboundChannel = serverCtx.channel();
            if (outboundChannel.isActive()) {
                outboundChannel.writeAndFlush(Unpooled.EMPTY_BUFFER)
                        .addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    @Override
    public String toString() {
        // Don't include StateHolder's toString here
        // because StateHolder's toString will include the backends's toString
        // and we don't want a SOE.
        return "KafkaProxyBackendHandler{" +
                ", serverCtx=" + serverCtx +
                ", pendingServerFlushes=" + pendingServerFlushes +
                '}';
    }
}
