/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.test.codec.DecodedRequestFrame;
import io.kroxylicious.test.codec.DecodedResponseFrame;

/**
 * MockHandler is responsible for:
 * <ol>
 *     <li>Serves a single response for any requests it receives. The response can be modified
 *  * using setResponse.</li>
 *     <li>Records requests it receives so they can be retrieved and verified</li>
 *     <li>Can be cleared, making it forget received requests</li>
 * </ol>
 */
@Sharable
public class MockHandler extends ChannelInboundHandlerAdapter {

    private ApiMessage message;

    private final List<DecodedRequestFrame<?>> requests = new ArrayList<>();

    /**
     * Create mockhandler with initial message to serve
     * @param message message to respond with, nullable
     */
    public MockHandler(ApiMessage message) {
        this.message = message;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        DecodedRequestFrame<?> msg1 = (DecodedRequestFrame<?>) msg;
        respond(ctx, msg1);
    }

    private void respond(ChannelHandlerContext ctx, DecodedRequestFrame<?> frame) {
        requests.add(frame);
        if (message == null) {
            // we allow a null message to enable tests to start the mock server
            // before they set the expectation.
            throw new RuntimeException("response message not set");
        }
        DecodedResponseFrame<?> responseFrame = new DecodedResponseFrame<>(frame.apiVersion(),
                frame.correlationId(), new ResponseHeaderData().setCorrelationId(frame.correlationId()), message);
        ctx.write(responseFrame);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }

    /**
     * Set the response
     * @param response response
     */
    public void setResponse(ApiMessage response) {
        message = response;
    }

    /**
     * Get requests
     * @return get received requests
     */
    public List<DecodedRequestFrame<?>> getRequests() {
        return Collections.unmodifiableList(requests);
    }

    /**
     * Clear recorded requests
     */
    public void clear() {
        requests.clear();
    }
}
