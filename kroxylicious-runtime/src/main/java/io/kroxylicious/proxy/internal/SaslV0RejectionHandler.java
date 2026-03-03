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

/**
 * Short circuit respond with an error code if we encounter a SASL v0 handshake, logging a meaningful warning and closing
 * the connection. The handler deregisters itself on the first RPC that is neither an API_VERSIONS request nor a v0 SASL
 * handshake as we presume we have successfully progressed beyond SASL authentication.
 * <p>
 * The proxy currently does not support v0 SASL handshakes where the client sends through SASL messages without framing them using
 * the Kafka protocol. Support for this is ancient, Kafka 4.0.0 preserved this mechanism due to a decision to support common
 * clients from 4 years prior to the release (python-kafka). We have also encountered a client (kaf) that configures sarama
 * to use v0 by default. These interactions resulted in an esoteric looking exception in the proxy when we attempted to decode
 * the SASL frame as a kafka protocol RPC. So this handler is intended to make it fail early with a clear signal in the proxy logs
 * and response that we can't handle SASL v0.
 * </p>
 */
public class SaslV0RejectionHandler extends ChannelDuplexHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SaslV0RejectionHandler.class);

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
            if (frame.apiKeyId() != ApiKeys.API_VERSIONS.id) {
                // we have advanced beyond api versions negotiation and are either in a non-v0 sasl negotiation, or some other RPC
                ctx.channel().pipeline().remove(this);
            }
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
