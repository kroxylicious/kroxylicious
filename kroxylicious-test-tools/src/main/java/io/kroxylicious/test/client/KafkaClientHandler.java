/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.test.codec.DecodedRequestFrame;
import io.kroxylicious.test.codec.DecodedResponseFrame;

/**
 * Sends a single request frame, waits for a response then closes the channel
 */
public class KafkaClientHandler extends ChannelInboundHandlerAdapter {
    private final List<DecodedRequestFrame<?>> decodedRequestFrames;
    private final DecodedRequestFrame<?> initialRequest;
    List<DecodedResponseFrame<?>> responses = new ArrayList<>();
    private final CompletableFuture<List<DecodedResponseFrame<?>>> onResponse = new CompletableFuture<>();
    private boolean readyToSendRemainingMessages;

    /**
     * Creates a KafkaClientHandler
     * @param decodedRequestFrames the requests to send
     */
    public KafkaClientHandler(boolean waitForFirstResponse, List<DecodedRequestFrame<?>> decodedRequestFrames) {
        this.decodedRequestFrames = decodedRequestFrames;
        this.initialRequest = waitForFirstResponse ? decodedRequestFrames.remove(0) : null;
        readyToSendRemainingMessages = !waitForFirstResponse;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (initialRequest != null) {
            ctx.writeAndFlush(initialRequest);
        }
        else {
            writeAllMessages(ctx);
        }
    }

    private void writeAllMessages(ChannelHandlerContext ctx) {
        for (DecodedRequestFrame<?> decodedRequestFrame : decodedRequestFrames) {
            ctx.write(decodedRequestFrame);
        }
        ctx.flush();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!readyToSendRemainingMessages) {
            readyToSendRemainingMessages = true;
            writeAllMessages(ctx);
        }
        else {
            responses.add((DecodedResponseFrame<?>) msg);
            if (allResponsesReceived()) {
                onResponse.complete(responses);
                ctx.close();
            }
        }
    }

    private boolean allResponsesReceived() {
        return responses.size() == decodedRequestFrames.size();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    /**
     * A future that is completed when the response is received
     * @return on response future
     */
    public CompletableFuture<List<DecodedResponseFrame<?>>> getOnResponseFuture() {
        return onResponse;
    }
}
