/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.ChannelFutureListener;
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
    private final CompletableFuture<Void> initialResponseReceived = new CompletableFuture<>();

    /**
     * Creates a KafkaClientHandler
     * @param decodedRequestFrames the requests to send
     */
    public KafkaClientHandler(DecodedRequestFrame<?> initialRequest, List<DecodedRequestFrame<?>> decodedRequestFrames) {
        this.decodedRequestFrames = decodedRequestFrames;
        this.initialRequest = initialRequest;
        if (this.initialRequest == null) {
            initialResponseReceived.complete(null);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (initialRequest != null) {
            ctx.writeAndFlush(initialRequest);
        }
        initialResponseReceived.thenAccept(unused -> {
            writeAllMessages(ctx);
        });
    }

    private void writeAllMessages(ChannelHandlerContext ctx) {
        for (DecodedRequestFrame<?> decodedRequestFrame : decodedRequestFrames) {
            ctx.write(decodedRequestFrame);
        }
        ctx.flush();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!initialResponseReceived.isDone()) {
            initialResponseReceived.complete(null);
        }
        else {
            responses.add((DecodedResponseFrame<?>) msg);
            if (allResponsesReceived()) {
                onResponse.complete(responses);
                ctx.write(msg).addListener(ChannelFutureListener.CLOSE);
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
