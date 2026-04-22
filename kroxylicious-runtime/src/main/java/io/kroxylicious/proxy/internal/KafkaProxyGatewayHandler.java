/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.Optional;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaProxyGatewayHandler extends ChannelInboundHandlerAdapter {

    private final ProxyChannelStateMachine proxyChannelStateMachine;
    @Nullable
    private ChannelHandlerContext clientCtx;

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

}
