/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.client;

import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.kafka.common.requests.ProduceRequest;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.test.codec.DecodedRequestFrame;
import io.kroxylicious.test.codec.DecodedResponseFrame;

/**
 * Simple kafka handle capable of sending one or more requests to a server side.
 * <br/>
 * In single-shot mode, the client closes the channel after the response is received.
 */
public class KafkaClientHandler extends ChannelInboundHandlerAdapter {
    private final Deque<DecodedRequestFrame<?>> queue = new ConcurrentLinkedDeque<>();
    private final boolean singleShot;
    private ChannelHandlerContext ctx;

    /**
     * Creates a KafkaClientHandler
     */
    public KafkaClientHandler(boolean singleShot) {
        this.singleShot = singleShot;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        ctx.fireChannelRegistered();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        processPendingWrites();
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (singleShot) {
            ctx.channel().close();
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }

    public CompletableFuture<DecodedResponseFrame<?>> sendRequest(DecodedRequestFrame<?> decodedRequestFrame) {
        queue.addLast(decodedRequestFrame);
        processPendingWrites();
        return decodedRequestFrame.getResponseFuture();
    }

    private void processPendingWrites() {
        ctx.executor().execute(() -> {
            if (ctx.channel().isActive()) {
                while (queue.peek() != null) {
                    DecodedRequestFrame<?> msg = queue.removeFirst();
                    ctx.writeAndFlush(msg).addListener(c -> {
                        if (msg.body() instanceof ProduceRequest && ((ProduceRequest) msg.body()).acks() == 0) {
                            msg.getResponseFuture().complete(null);
                        }
                    });
                }
            }
        });
    }

}
