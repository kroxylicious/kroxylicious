/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;

import io.kroxylicious.proxy.config.ProxyProtocolMode;

/**
 * Uses {@link HAProxyMessageDecoder#detectProtocol(ByteBuf)} on the first bytes
 * of a connection to decide whether to install the PROXY protocol decoder.
 * <p>
 * In {@link ProxyProtocolMode#REQUIRED} mode, non-PROXY bytes cause the
 * connection to be closed with a warning. In {@link ProxyProtocolMode#AUTO}
 * mode, non-PROXY bytes are simply passed through to the Kafka decoder.
 * </p>
 * <p>
 * This handler removes itself from the pipeline after the first read.
 * </p>
 */
public class HaProxyProtocolDetectionHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HaProxyProtocolDetectionHandler.class);

    private static final String HAPROXY_MESSAGE_DECODER_HANDLER_NAME = "HAProxyMessageDecoder";
    private static final String HAPROXY_MESSAGE_HANDLER_NAME = "HaProxyMessageHandler";

    private final ProxyProtocolMode mode;
    private final ProxyChannelStateMachine proxyChannelStateMachine;

    public HaProxyProtocolDetectionHandler(ProxyProtocolMode mode, ProxyChannelStateMachine proxyChannelStateMachine) {
        this.mode = mode;
        this.proxyChannelStateMachine = proxyChannelStateMachine;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ByteBuf buf)) {
            super.channelRead(ctx, msg);
            return;
        }

        var result = HAProxyMessageDecoder.detectProtocol(buf);
        boolean proxyDetected = switch (result.state()) {
            case DETECTED -> true;
            case INVALID -> false;
            case NEEDS_MORE_DATA -> false;
        };

        if (proxyDetected) {
            LOGGER.debug("{}: PROXY protocol detected, adding decoder", ctx.channel());
            String thisName = ctx.name();
            ctx.pipeline().addAfter(thisName, HAPROXY_MESSAGE_DECODER_HANDLER_NAME, new HAProxyMessageDecoder());
            ctx.pipeline().addAfter(HAPROXY_MESSAGE_DECODER_HANDLER_NAME, HAPROXY_MESSAGE_HANDLER_NAME,
                    new HaProxyMessageHandler(proxyChannelStateMachine));
            ctx.pipeline().remove(this);
            ctx.fireChannelRead(buf);
        }
        else if (mode == ProxyProtocolMode.REQUIRED) {
            LOGGER.warn("{}: Connection rejected — expected PROXY protocol header but received non-PROXY data. "
                    + "Ensure the upstream load balancer is configured to send PROXY protocol headers, "
                    + "or set proxyProtocol mode to 'auto' or 'disabled'.",
                    ctx.channel());
            buf.release();
            ctx.close();
        }
        else {
            // AUTO mode — no PROXY header, pass through to Kafka decoder
            LOGGER.debug("{}: No PROXY protocol header detected, passing through", ctx.channel());
            ctx.pipeline().remove(this);
            ctx.fireChannelRead(buf);
        }
    }
}
