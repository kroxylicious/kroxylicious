/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.tag.VisibleForTesting;

public class KafkaProxyBackendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyBackendHandler.class);

    @VisibleForTesting
    final ProxyChannelStateMachine proxyChannelStateMachine;
    @VisibleForTesting
    final SslContext sslContext;
    ChannelHandlerContext serverCtx;
    private boolean pendingServerFlushes;

    public KafkaProxyBackendHandler(ProxyChannelStateMachine proxyChannelStateMachine, VirtualCluster virtualCluster) {
        this.proxyChannelStateMachine = Objects.requireNonNull(proxyChannelStateMachine);
        Optional<SslContext> upstreamSslContext = virtualCluster.getUpstreamSslContext();
        this.sslContext = upstreamSslContext.orElse(null);
    }

    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        if (ctx.channel().isWritable()) {
            proxyChannelStateMachine.onServerWritable();
        }
        else {
            proxyChannelStateMachine.onServerUnwritable();
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        this.serverCtx = ctx;
        super.channelRegistered(ctx);
    }

    // Called when the outbound channel is active
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.trace("Channel active {}", ctx);
        if (sslContext == null) {
            proxyChannelStateMachine.onServerActive();
        }
        super.channelActive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SslHandshakeCompletionEvent sslEvt) {
            if (sslEvt.isSuccess()) {
                proxyChannelStateMachine.onServerActive();
            }
            else {
                proxyChannelStateMachine.onServerException(sslEvt.cause());
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        proxyChannelStateMachine.onServerInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        proxyChannelStateMachine.onServerException(cause);
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
        proxyChannelStateMachine.forwardToClient(msg);
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        proxyChannelStateMachine.serverReadComplete();
    }

    public void forwardToServer(Object msg) {
        if (serverCtx == null) {
            // TODO this shouldn't really be possible to reach. Delete in a releases time once we have more confidence in the StateMachine
            proxyChannelStateMachine.illegalState("write without outbound active outbound channel");
        }
        else {
            final Channel outboundChannel = serverCtx.channel();
            if (outboundChannel.isWritable()) {
                outboundChannel.write(msg, serverCtx.voidPromise());
                pendingServerFlushes = true;
            }
            else {
                outboundChannel.writeAndFlush(msg, serverCtx.voidPromise());
                pendingServerFlushes = false;
            }
        }
        LOGGER.trace("/READ");
    }

    public void flushToServer() {
        if (serverCtx != null) {
            final Channel serverChannel = serverCtx.channel();
            if (pendingServerFlushes) {
                pendingServerFlushes = false;
                serverChannel.flush();
            }
            if (!serverChannel.isWritable()) {
                proxyChannelStateMachine.onServerUnwritable();
            }
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

    public void inClosed() {
        if (serverCtx != null) {
            Channel outboundChannel = serverCtx.channel();
            if (outboundChannel.isActive()) {
                outboundChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    @Override
    public String toString() {
        // Don't include proxyChannelStateMachine's toString here
        // because proxyChannelStateMachine's toString will include the backends toString
        // and we don't want a SOE.
        return "KafkaProxyBackendHandler{" + ", serverCtx=" + serverCtx + ", proxyChannelState=" + this.proxyChannelStateMachine.currentState()
                + ", pendingServerFlushes=" + pendingServerFlushes + '}';
    }
}
