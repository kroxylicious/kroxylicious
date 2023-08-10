/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.client;

import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClientHandler.class);

    private final Deque<DecodedRequestFrame<?>> queue = new ConcurrentLinkedDeque<>();
    private final boolean singleShot;
    private ChannelHandlerContext ctx;

    // Read/Mutated by the Netty thread only.
    private boolean channelActivationSeen;

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
        this.channelActivationSeen = true;
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
        LOGGER.warn("Kafka test client received unexpected exception, closing connection.", cause);
        ctx.close();
    }

    /**
     * Sends a request frame.  If the channel is not yet active, the request is queued up until it
     * is.
     * <br/>
     * The response to the request is returned by the future. If the request has no response the
     * future will complete once the request is sent and yield a null value.
     *
     * @param decodedRequestFrame request frame to send
     * @return future that will yield the response.
     */
    public CompletableFuture<DecodedResponseFrame<?>> sendRequest(DecodedRequestFrame<?> decodedRequestFrame) {
        queue.addLast(decodedRequestFrame);
        processPendingWrites();
        return decodedRequestFrame.getResponseFuture();
    }

    private void processPendingWrites() {
        ctx.executor().execute(() -> {
            if (!channelActivationSeen) {
                return;
            }

            while (queue.peek() != null) {
                var msg = queue.removeFirst();
                ctx.writeAndFlush(msg).addListener(c -> {
                    var responseFuture = msg.getResponseFuture();
                    if (c.cause() != null) {
                        // I/O failed etc
                        responseFuture.completeExceptionally(c.cause());
                    }
                    else if (!msg.hasResponse()) {
                        responseFuture.complete(null);
                    }
                });
            }
        });
    }

}
