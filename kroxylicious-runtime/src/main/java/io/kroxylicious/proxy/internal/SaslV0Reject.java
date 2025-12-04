/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;

public class SaslV0Reject extends ChannelDuplexHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SaslV0Reject.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Frame frame = (Frame) msg;
        if (frame.apiKeyId() == ApiKeys.SASL_HANDSHAKE.id && frame.apiVersion() == (short) 0) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("{}: SASL V0 handshake is not implemented by the proxy, your client is using an ancient SASL api version", ctx.channel().id());
            }
            ctx.writeAndFlush(unsupportedVersionFrame(frame)).addListener(ChannelFutureListener.CLOSE);
        }
        else {
            super.channelRead(ctx, msg);
        }
    }

    private static DecodedResponseFrame<SaslHandshakeResponseData> unsupportedVersionFrame(Frame frame) {
        SaslHandshakeResponseData saslHandshakeResponseData = new SaslHandshakeResponseData();
        saslHandshakeResponseData.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        return new DecodedResponseFrame<>((short) 0, frame.correlationId(),
                new ResponseHeaderData().setCorrelationId(frame.correlationId()),
                saslHandshakeResponseData);
    }
}
