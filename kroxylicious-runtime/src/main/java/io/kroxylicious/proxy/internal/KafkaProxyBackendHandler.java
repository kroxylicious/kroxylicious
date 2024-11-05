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

    /**
     * Propagates backpressure to the <em>downstream/client</em> connection by notifying the {@link ProxyChannelStateMachine} when the <em>upstream/server</em> connection
     * blocks or unblocks.
     * @param ctx the handler context for upstream/server channel
     * @throws Exception If something went wrong
     */
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

    /**
     * Netty callback that resources have been allocated for the channel.
     * This is the first point at which we become aware of the upstream/server channel.
     * @param ctx the context for the upstream/server channel.
     * @throws Exception If something went wrong.
     */
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        this.serverCtx = ctx;
        super.channelRegistered(ctx);
    }

    /**
     * Netty callback that upstream/server channel has successfully connected to the remote peer.
     * This does not mean that the channel is usable by the proxy as TLS negotiation, if required, is still in progress.
     * @param ctx the context for the upstream/server channel.
     * @throws Exception If something went wrong.
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.trace("Channel active {}", ctx);
        if (sslContext == null) {
            proxyChannelStateMachine.onServerActive();
        }
        super.channelActive(ctx);
    }

    /**
     * Netty callback. Used to notify us of custom events.
     * Events such as the SSL Handshake completing.
     * <br>
     * This method is called for <em>every</em> custom event, so its up to us to filter out the ones we care about.
     *
     * @param ctx the channel handler context on which the event was triggered.
     * @param event the information being notified
     * @throws Exception any errors in processing.
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof SslHandshakeCompletionEvent sslEvt) {
            if (sslEvt.isSuccess()) {
                proxyChannelStateMachine.onServerActive();
            }
            else {
                proxyChannelStateMachine.onServerException(sslEvt.cause());
            }
        }
        super.userEventTriggered(ctx, event);
    }

    /**
     * Netty callback to notify that the <em>upstream/server</em> channel TCP connection has disconnected.
     * @param ctx The context for the upstream/server channel.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        proxyChannelStateMachine.onServerInactive();
    }

    /**
     * Netty callback indicating that an exception reached it.
     * Which means the proxy should give up as all hope for this connection is lost.
     * @param ctx The context for the upstream/server channel.
     * @param cause The exception which reached netty.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        proxyChannelStateMachine.onServerException(cause);
    }

    /**
     * Netty callback that something has been read from the <em>upstream/server</em> channel.
     * There may be further calls to this method before a call to {@code channelReadComplete()} 
     * signals the end of the current read operation.
     * @param ctx The context for the upstream/server channel.
     * @param msg the message read from the channel.
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        proxyChannelStateMachine.messageFromServer(msg);
    }

    /**
     * <p>Invoked when the last message read by the current read operation
     * has been consumed by {@link #channelRead(ChannelHandlerContext, Object)}.</p>
     * This allows the proxy to batch requests.
     * @param ctx The upstream/server context
     */
    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        proxyChannelStateMachine.serverReadComplete();
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} to propagate an RPC to the upstream node.
     * @param msg the RPC to forward.
     */
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

    /**
     * Called by the {@link ProxyChannelStateMachine} when the batch from the downstream/client side is complete.
     */
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

    /**
     * Callback from the {@link ProxyChannelStateMachine} triggered when it wants to apply backpressure to the <em>upstream/server</em> connection
     */
    public void blockServerReads() {
        if (serverCtx != null) {
            serverCtx.channel().config().setAutoRead(false);
        }
    }

    /**
     * Callback from the {@link ProxyChannelStateMachine} triggered when it wants to remove backpressure from the <em>upstream/server</em> connection
     */
    public void unblockServerReads() {
        if (serverCtx != null) {
            serverCtx.channel().config().setAutoRead(true);
        }
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link ProxyChannelState.Closed} state.
     */
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
        // because proxyChannelStateMachine's toString will include the backend's toString
        // and we don't want a SOE.
        return "KafkaProxyBackendHandler{" + ", serverCtx=" + serverCtx + ", proxyChannelState=" + this.proxyChannelStateMachine.currentState()
                + ", pendingServerFlushes=" + pendingServerFlushes + '}';
    }
}
