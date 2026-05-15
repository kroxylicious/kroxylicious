/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaProxyBackendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyBackendHandler.class);

    @VisibleForTesting
    final ServerConnectionStateMachine serverConnectionStateMachine;
    @Nullable
    ChannelHandlerContext serverCtx;
    private boolean pendingServerFlushes;

    KafkaProxyBackendHandler(ServerConnectionStateMachine serverConnectionStateMachine) {
        this.serverConnectionStateMachine = Objects.requireNonNull(serverConnectionStateMachine);
    }

    /**
     * Propagates backpressure to the <em>downstream/client</em> connection by notifying the {@link ServerConnectionStateMachine} when the <em>upstream/server</em> connection
     * blocks or unblocks.
     * @param ctx the handler context for upstream/server channel
     * @throws Exception If something went wrong
     */
    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        if (ctx.channel().isWritable()) {
            serverConnectionStateMachine.onServerWritable();
        }
        else {
            serverConnectionStateMachine.onServerUnwritable();
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
        LOGGER.atTrace()
                .addKeyValue("context", ctx)
                .log("Channel active");
        if (!serverConnectionStateMachine.isUpstreamTls()) {
            serverConnectionStateMachine.onServerActive();
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
                serverConnectionStateMachine.onServerActive();
            }
            else {
                serverConnectionStateMachine.onServerException(sslEvt.cause());
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
        serverConnectionStateMachine.onServerInactive();
    }

    /**
     * Netty callback indicating that an exception reached it.
     * Which means the proxy should give up as all hope for this connection is lost.
     * @param ctx The context for the upstream/server channel.
     * @param cause The exception which reached netty.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        serverConnectionStateMachine.onServerException(cause);
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
        serverConnectionStateMachine.onMessageFromServer(msg);
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
        serverConnectionStateMachine.serverReadComplete();
    }

    /**
     * Called by the {@link ServerConnectionStateMachine} to propagate an RPC to the upstream node.
     * @param msg the RPC to forward.
     */
    void forwardToServer(Object msg) {
        if (serverCtx == null) {
            serverConnectionStateMachine.onServerException(
                    new IllegalStateException("write without active outbound channel"));
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
        LOGGER.atTrace().log("/READ");
    }

    /**
     * Called by the {@link ServerConnectionStateMachine} when the batch from the downstream/client side is complete.
     */
    void flushToServer() {
        if (serverCtx != null) {
            final Channel serverChannel = serverCtx.channel();
            if (pendingServerFlushes) {
                pendingServerFlushes = false;
                serverChannel.flush();
            }
            if (!serverChannel.isWritable()) {
                serverConnectionStateMachine.onServerUnwritable();
            }
        }
    }

    /**
     * Callback from the {@link ServerConnectionStateMachine} triggered when it wants to apply backpressure to the <em>upstream/server</em> connection
     */
    void applyBackpressure() {
        if (serverCtx != null) {
            serverCtx.channel().config().setAutoRead(false);
        }
    }

    /**
     * Callback from the {@link ServerConnectionStateMachine} triggered when it wants to remove backpressure from the <em>upstream/server</em> connection
     */
    void relieveBackpressure() {
        if (serverCtx != null) {
            serverCtx.channel().config().setAutoRead(true);
        }
    }

    /**
     * Returns the upstream (proxy→Kafka) channel, or null if not yet registered.
     */
    @Nullable
    Channel serverChannel() {
        return serverCtx != null ? serverCtx.channel() : null;
    }

    /**
     * Called by the {@link ServerConnectionStateMachine} on entry to the {@link ServerConnectionState.Closed} state.
     */
    void inClosed() {
        if (serverCtx != null) {
            Channel outboundChannel = serverCtx.channel();
            if (outboundChannel.isActive()) {
                outboundChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    @Override
    public String toString() {
        return "KafkaProxyBackendHandler{serverCtx=" + serverCtx
                + ", serverConnectionState=" + serverConnectionStateMachine.state()
                + ", pendingServerFlushes=" + pendingServerFlushes + '}';
    }
}
