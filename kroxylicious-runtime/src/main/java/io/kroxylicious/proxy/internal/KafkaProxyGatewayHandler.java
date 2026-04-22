/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.apache.kafka.common.message.ResponseHeaderData;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.ResponseFrame;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaProxyGatewayHandler extends ChannelInboundHandlerAdapter {

    private final ProxyChannelStateMachine proxyChannelStateMachine;
    @Nullable
    private ChannelHandlerContext clientCtx;

    @VisibleForTesting
    @Nullable
    List<Object> bufferedMsgs;

    public KafkaProxyGatewayHandler(ProxyChannelStateMachine proxyChannelStateMachine) {
        Objects.requireNonNull(proxyChannelStateMachine, "state machine was null");
        this.proxyChannelStateMachine = proxyChannelStateMachine;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.clientCtx = ctx;
        proxyChannelStateMachine.onClientActive(this);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        proxyChannelStateMachine.onClientInactive();
        super.channelInactive(ctx);
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} when there is a requirement to buffer RPC's prior to forwarding to the upstream/server.
     * Generally this is expected to be when client requests are received before we have a connection to the upstream node.
     * @param msg the RPC to buffer.
     */
    void bufferMsg(Object msg) {
        if (bufferedMsgs == null) {
            bufferedMsgs = new ArrayList<>();
        }
        bufferedMsgs.add(msg);
    }

    /**
     * Netty callback. Used to notify us of custom events.
     * Events such as ServerNameIndicator (SNI) resolution completing.
     * <br>
     * This method is called for <em>every</em> custom event, so its up to us to filter out the ones we care about.
     *
     * @param ctx the channel handler context on which the event was triggered.
     * @param event the information being notified
     * @throws Exception any errors in processing.
     */
    @Override
    public void userEventTriggered(
                                   ChannelHandlerContext ctx,
                                   Object event)
            throws Exception {
        if (event instanceof SslHandshakeCompletionEvent handshakeCompletionEvent
                && handshakeCompletionEvent.isSuccess()) {
            this.proxyChannelStateMachine.onClientTlsHandshakeSuccess(sslSession());
        }
        super.userEventTriggered(ctx, event);
    }

    @Nullable
    SSLSession sslSession() {
        // The SslHandler is added to the pipeline by the SniHandler (replacing it) after the ClientHello.
        // It is added using the fully-qualified class name.
        SslHandler sslHandler = (SslHandler) clientCtx().pipeline().get(SslHandler.class.getName());
        return Optional.ofNullable(sslHandler)
                .map(SslHandler::engine)
                .map(SSLEngine::getSession)
                .orElse(null);
    }

    private ChannelHandlerContext clientCtx() {
        return Objects.requireNonNull(clientCtx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        proxyChannelStateMachine.onClientRequestGateway(msg);
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link ProxyChannelState.Forwarding} state.
     */
    void inForwarding() {
        // connection is complete, so first forward the buffered message
        if (bufferedMsgs != null) {
            for (Object bufferedMsg : bufferedMsgs) {
                this.clientCtx().fireChannelRead(bufferedMsg);
            }
            bufferedMsgs = null;
        }
    }

    public void forward(Object msg) {
        clientCtx().fireChannelRead(msg);
    }

    private static ResponseFrame buildErrorResponseFrame(
                                                         DecodedRequestFrame<?> triggerFrame,
                                                         Throwable error) {
        var responseData = KafkaProxyExceptionMapper.errorResponseMessage(triggerFrame, error);
        final ResponseHeaderData responseHeaderData = new ResponseHeaderData();
        responseHeaderData.setCorrelationId(triggerFrame.correlationId());
        return new DecodedResponseFrame<>(triggerFrame.apiVersion(), triggerFrame.correlationId(), responseHeaderData, responseData);
    }

    /**
     * Return an error response to send to the client, or null if no response should be sent.
     * @param errorCodeEx The exception
     * @return The response frame
     */
    private @Nullable ResponseFrame errorResponse(
                                                  @Nullable Throwable errorCodeEx) {
        ResponseFrame errorResponse;
        final Object triggerMsg = ((bufferedMsgs != null) && !bufferedMsgs.isEmpty()) ? bufferedMsgs.get(0) : null;
        if (errorCodeEx != null && triggerMsg instanceof final DecodedRequestFrame<?> triggerFrame) {
            errorResponse = buildErrorResponseFrame(triggerFrame, errorCodeEx);
        }
        else {
            errorResponse = null;
        }
        return errorResponse;
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link ProxyChannelState.Closed} state.
     */
    void inClosed(@Nullable Throwable errorCodeEx) {
        Channel inboundChannel = clientCtx().channel();
        if (inboundChannel.isActive()) {
            Object msg = null;
            if (errorCodeEx != null) {
                msg = errorResponse(errorCodeEx);
            }
            if (msg == null) {
                msg = Unpooled.EMPTY_BUFFER;
            }
            inboundChannel.writeAndFlush(msg)
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public String toString() {
        // Don't include proxyChannelStateMachine's toString here
        // because proxyChannelStateMachine's toString will include the gateway's toString
        // and we don't want a SOE.
        return "KafkaProxyFrontendHandler{"
                + ", clientCtx=" + clientCtx
                + ", proxyChannelState=" + this.proxyChannelStateMachine.currentState()
                + ", number of bufferedMsgs=" + (bufferedMsgs == null ? 0 : bufferedMsgs.size())
                + '}';
    }
}
