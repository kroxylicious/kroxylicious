/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.client;

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
    private final DecodedRequestFrame<?> decodedRequestFrame;
    private final CompletableFuture<DecodedResponseFrame<?>> onResponse = new CompletableFuture<>();

    /**
     * Creates a KafkaClientHandler
     * @param decodedRequestFrame the single request to send
     */
    public KafkaClientHandler(DecodedRequestFrame<?> decodedRequestFrame) {
        this.decodedRequestFrame = decodedRequestFrame;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(decodedRequestFrame);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        onResponse.complete((DecodedResponseFrame<?>) msg);
        ctx.write(msg).addListener(ChannelFutureListener.CLOSE);
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
    public CompletableFuture<DecodedResponseFrame<?>> getOnResponseFuture() {
        return onResponse;
    }
}
