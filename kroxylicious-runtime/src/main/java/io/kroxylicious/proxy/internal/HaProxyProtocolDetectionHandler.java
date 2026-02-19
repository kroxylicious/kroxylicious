/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;

import io.kroxylicious.proxy.config.ProxyProtocolMode;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Uses {@link HAProxyMessageDecoder#detectProtocol(ByteBuf)} on the first bytes
 * of a connection to decide whether to install the PROXY protocol decoder.
 * <p>
 * In {@link ProxyProtocolMode#REQUIRED} mode, non-PROXY bytes cause the
 * connection to be closed with a warning. In {@link ProxyProtocolMode#ALLOWED}
 * mode, non-PROXY bytes are simply passed through to the Kafka decoder.
 * </p>
 * <p>
 * This handler is not used when the proxy is in {@link ProxyProtocolMode#DISABLED} mode.
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
    private final KafkaSession kafkaSession;
    @Nullable
    private ByteBuf cumulation;

    public HaProxyProtocolDetectionHandler(ProxyProtocolMode mode, KafkaSession kafkaSession) {
        this.mode = mode;
        this.kafkaSession = kafkaSession;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ByteBuf buf)) {
            super.channelRead(ctx, msg);
            return;
        }

        if (cumulation == null) {
            cumulation = buf;
        }
        else {
            cumulation = Unpooled.wrappedBuffer(cumulation, buf);
        }

        var result = HAProxyMessageDecoder.detectProtocol(cumulation);
        switch (result.state()) {
            case DETECTED -> {
                LOGGER.atDebug().addKeyValue("ctx", ctx.channel()).log("PROXY protocol detected, adding decoder");
                String thisName = ctx.name();
                ctx.pipeline().addAfter(thisName, HAPROXY_MESSAGE_DECODER_HANDLER_NAME, new HAProxyMessageDecoder());
                ctx.pipeline().addAfter(HAPROXY_MESSAGE_DECODER_HANDLER_NAME, HAPROXY_MESSAGE_HANDLER_NAME,
                        new HaProxyMessageHandler(kafkaSession));
                ctx.pipeline().remove(this);
                ctx.fireChannelRead(cumulation);
                cumulation = null;
            }
            case INVALID -> {
                if (mode == ProxyProtocolMode.REQUIRED) {
                    LOGGER.atWarn().addKeyValue("ctx", ctx.channel())
                            .log("Connection rejected — expected PROXY protocol header but received non-PROXY data. "
                                    + "Ensure the upstream load balancer is configured to send PROXY protocol headers, "
                                    + "or set proxyProtocol mode to 'allowed' or 'disabled'.");
                    cumulation.release();
                    cumulation = null;
                    ctx.close();
                }
                else {
                    // ALLOWED mode — no PROXY header, pass through to Kafka decoder
                    LOGGER.atDebug().addKeyValue("ctx", ctx.channel()).log("No PROXY protocol header detected, passing through");
                    ctx.pipeline().remove(this);
                    ctx.fireChannelRead(cumulation);
                    cumulation = null;
                }
            }
            case NEEDS_MORE_DATA -> {
                LOGGER.atDebug().addKeyValue("ctx", ctx.channel()).log("PROXY protocol detection needs more data, waiting for next read");
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (cumulation != null) {
            cumulation.release();
            cumulation = null;
        }
        super.channelInactive(ctx);
    }
}
