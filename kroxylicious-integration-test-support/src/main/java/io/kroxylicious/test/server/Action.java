/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.server;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.test.codec.DecodedRequestFrame;
import io.kroxylicious.test.codec.DecodedResponseFrame;

interface Action {
    void handle(ChannelHandlerContext ctx, DecodedRequestFrame<?> frame);

    static Action drop() {
        return (ctx, frame) -> {
        };
    }

    static Action respond(ApiMessage message) {
        return (ctx, frame) -> {
            writeResponse(message, ctx, frame, frame.apiVersion());
        };
    }

    static Action respond(ApiMessage message, short apiVersion) {
        return (ctx, frame) -> {
            writeResponse(message, ctx, frame, apiVersion);
        };
    }

    private static void writeResponse(ApiMessage message, ChannelHandlerContext ctx, DecodedRequestFrame<?> frame, short apiVersion) {
        DecodedResponseFrame<?> responseFrame = new DecodedResponseFrame<>(apiVersion,
                frame.correlationId(), new ResponseHeaderData().setCorrelationId(frame.correlationId()), message);
        ctx.write(responseFrame);
    }

}
