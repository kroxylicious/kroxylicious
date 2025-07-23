/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.test.Request;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.codec.DecodedRequestFrame;

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

    private record ConditionalMockResponse(Matcher<Request> matcher, Action action, AtomicLong invocations) {

    }

    private static final Logger logger = LoggerFactory.getLogger(MockHandler.class);

    private final List<ConditionalMockResponse> conditionalMockResponses = new ArrayList<>();

    private final List<DecodedRequestFrame<?>> requests = new ArrayList<>();

    /**
     * Create mockhandler with initial message to serve
     * @param payload payload to respond with, nullable
     */
    public MockHandler(ResponsePayload payload) {
        if (payload != null && payload.message() != null) {
            ApiMessage message = payload.message();
            setMockResponseForApiKey(ApiKeys.forId(message.apiKey()), message, payload.responseApiVersion());
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        DecodedRequestFrame<?> msg1 = (DecodedRequestFrame<?>) msg;
        respond(ctx, msg1);
    }

    private void respond(ChannelHandlerContext ctx, DecodedRequestFrame<?> frame) {
        requests.add(frame);
        ConditionalMockResponse response = conditionalMockResponses.stream().filter(r -> r.matcher.matches(MockServer.toRequest(frame))).findFirst().orElseThrow();
        response.action.handle(ctx, frame);
        response.invocations.incrementAndGet();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Exception in Mock Handler", cause);
        // Close the connection when an exception is raised.
        ctx.close();
    }

    /**
     * Set the response
     *
     * @param response response
     */
    public void setMockResponseForApiKey(ApiKeys keys, ApiMessage response) {
        addMockResponse(new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Request request) {
                return request.apiKeys() == keys;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("has key " + keys);
            }
        }, Action.respond(response));
    }

    /**
     * Set the response
     *
     * @param response response
     * @param responseApiVersion apiVersion used to encode mock response
     */
    public void setMockResponseForApiKey(ApiKeys keys, ApiMessage response, short responseApiVersion) {
        addMockResponse(new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Request request) {
                return request.apiKeys() == keys;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("has key " + keys);
            }
        }, Action.respond(response, responseApiVersion));
    }

    public void addMockResponse(Matcher<Request> matcher, Action action) {
        conditionalMockResponses.add(new ConditionalMockResponse(matcher, action, new AtomicLong(0)));
    }

    /**
     * Get requests
     * @return get received requests
     */
    public List<DecodedRequestFrame<?>> getRequests() {
        return Collections.unmodifiableList(requests);
    }

    public void assertAllMockInteractionsInvoked() {
        List<ConditionalMockResponse> anyUninvoked = conditionalMockResponses.stream().filter(r -> r.invocations.get() <= 0).toList();
        if (!anyUninvoked.isEmpty()) {
            String collect = anyUninvoked.stream().map(conditionalMockResponse -> {
                StringDescription stringDescription = new StringDescription();
                conditionalMockResponse.matcher.describeTo(stringDescription);
                return "mock response was never invoked: " + stringDescription;
            }).collect(Collectors.joining(","));
            throw new AssertionError(collect);
        }
    }

    /**
     * Clear recorded requests
     */
    public void clear() {
        requests.clear();
        conditionalMockResponses.clear();
    }
}
