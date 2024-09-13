/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.RequestFrame;

/**
 * While processing a request from a Client, we want to enable custom Protocol
 * Filters to decide to send a response toward the Client instead of forwarding
 * the message on towards the upstream broker. In this case we may not be able
 * to immediately send the response to the Client. There may be other requests
 * in-flight that have already been forwarded to the upstream broker and the
 * Kafka protocol requires responses to be sent to the Client in the same order
 * they sent the requests.
 * This class tries to ensure that we respond to requests in the correct order by
 * remembering which correlationIds are in-flight and enqueuing any responses we
 * get that are out of order. Then, when we do encounter the response for the
 * oldest in-flight correlationId we can check if there are any enqueued
 * responses that can now be forwarded towards the client.
 */
public class ResponseOrderer extends ChannelDuplexHandler {

    Deque<Integer> inflightCorrelationIds = new ArrayDeque<>();
    Map<Integer, QueuedResponse> queuedResponses = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(ResponseOrderer.class);

    record QueuedResponse(
            Object msg,
            ChannelPromise promise
    ) {
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RequestFrame requestFrame) {
            if (requestFrame.hasResponse()) {
                inflightCorrelationIds.addLast(requestFrame.correlationId());
            }
        } else if (msg instanceof Frame frame) {
            inflightCorrelationIds.addLast(frame.correlationId());
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Frame responseFrame) {
            Integer oldestCorrelationId = inflightCorrelationIds.peekFirst();
            if (oldestCorrelationId == null) {
                logger.warn("Handling a Frame {}, but we have no inflight correlation ids, continuing to write", msg);
                super.write(ctx, msg, promise);
            } else if (oldestCorrelationId == responseFrame.correlationId()) {
                inflightCorrelationIds.removeFirst();
                super.write(ctx, msg, promise);
                drainQueue(ctx);
            } else {
                queuedResponses.put(responseFrame.correlationId(), new QueuedResponse(msg, promise));
            }
        } else {
            super.write(ctx, msg, promise);
        }
    }

    private void drainQueue(ChannelHandlerContext ctx) throws Exception {
        Integer oldestCorrelationId;
        while ((oldestCorrelationId = inflightCorrelationIds.peekFirst()) != null && queuedResponses.containsKey(oldestCorrelationId)) {
            Integer integer = inflightCorrelationIds.removeFirst();
            QueuedResponse thing = queuedResponses.remove(integer);
            super.write(ctx, thing.msg, thing.promise);
        }
    }

    int inFlightRequestCount() {
        return inflightCorrelationIds.size();
    }

    int queuedResponseCount() {
        return queuedResponses.size();
    }
}
