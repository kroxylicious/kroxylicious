/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.codec;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.frame.Frame;

/**
 * From the Kafka docs: The server guarantees that on a single TCP connection, requests will be processed
 * in the order they are sent and responses will return in that order as well.
 * If we want to do any asynchronous work in our Filter implementations before responding we must still
 * ensure that we respond in-order. To do this we:
 * <ol>
 *     <li>1. track the order we have received correlation ids</li>
 *     <li>2. when we receive a response, if it's not for the oldest correlation id, store that response in memory</li>
 *     <li>3. when we receive a response, if it's for the oldest correlation id, respond immediately
 *     and send any pending that are in the right order</li>
 * </ol>
 */
public class ResponseOrderingHandler extends ChannelDuplexHandler {

    Deque<Integer> requests = new LinkedList<>();
    Map<Integer, Frame> pendingFrames = new HashMap<>();

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Frame) {
            Integer firstCorrelationId = requests.getFirst();
            if (firstCorrelationId == ((Frame) msg).correlationId()) {
                requests.removeFirst();
                ctx.write(msg);
                sinkResponses(ctx);
            }
            else {
                pendingFrames.put(((Frame) msg).correlationId(), (Frame) msg);
            }
        }
        else {
            ctx.write(msg);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof Frame) {
            requests.addLast(((Frame) msg).correlationId());
        }
        ctx.fireChannelRead(msg);
    }

    private void sinkResponses(ChannelHandlerContext ctx) {
        while (!requests.isEmpty()) {
            Integer firstCorrelationId = requests.getFirst();
            if (pendingFrames.containsKey(firstCorrelationId)) {
                requests.removeFirst();
                ctx.write(pendingFrames.remove(firstCorrelationId));
            }
            else {
                break;
            }
        }
    }
}
