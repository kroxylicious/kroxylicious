/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.AfterEach;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.future.Promise;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * A test harness for {@link KrpcFilter} implementations.
 */
public abstract class FilterHarness {
    public static final String TEST_CLIENT = "test-client";
    protected EmbeddedChannel channel;
    private FilterHandler filterHandler;
    private KrpcFilter filter;

    /**
     * Build a {@link #channel} containing a single {@link FilterHandler} for the given
     * {@code filter}.
     * @param filter The filter in the pipeline.
     */
    protected void buildChannel(KrpcFilter filter) {
        buildChannel(filter, 1000L);
    }

    /**
     * Build a {@link #channel} containing a single {@link FilterHandler} for the given
     * {@code filter}.
     * @param filter The filter in the pipeline.
     * @param timeoutMs The timeout for {@link io.kroxylicious.proxy.filter.KrpcFilterContext#sendRequest(short, ApiMessage)}.
     */
    protected void buildChannel(KrpcFilter filter, long timeoutMs) {
        this.filter = filter;
        filterHandler = new FilterHandler(filter, timeoutMs, null, null);
        channel = new EmbeddedChannel(filterHandler);
    }

    /**
     * Write a client request to the pipeline.
     * @param data The request body.
     * @return The frame that was sent.
     * @param <B> The type of the request.
     */
    protected <B extends ApiMessage> DecodedRequestFrame<B> writeRequest(B data) {
        var apiKey = ApiKeys.forId(data.apiKey());
        var header = new RequestHeaderData();
        int correlationId = 42;
        header.setCorrelationId(correlationId);
        header.setRequestApiKey(apiKey.id);
        header.setRequestApiVersion(apiKey.latestVersion());
        header.setClientId(TEST_CLIENT);
        var frame = new DecodedRequestFrame<>(apiKey.latestVersion(), correlationId, false, header, data);
        channel.writeOutbound(frame);
        return frame;
    }

    /**
     * Write a normal client response, as if from the broker.
     * @param data The body of the response.
     * @return The frame that was written.
     * @param <B> The type of the response body.
     */
    protected <B extends ApiMessage> DecodedResponseFrame<B> writeResponse(B data) {
        var apiKey = ApiKeys.forId(data.apiKey());
        var header = new ResponseHeaderData();
        int correlationId = 42;
        header.setCorrelationId(correlationId);
        var frame = new DecodedResponseFrame<>(apiKey.latestVersion(), correlationId, header, data);
        channel.writeInbound(frame);
        return frame;
    }

    /**
     * Write a response for a filter-originated request, as if from the broker.
     * @param promise The promise that was returned from
     * {@link io.kroxylicious.proxy.filter.KrpcFilterContext#sendRequest(short, ApiMessage)}.
     * @param data The body of the response.
     * @return The frame that was written.
     * @param <B> The type of the response body.
     */
    protected <B extends ApiMessage> DecodedResponseFrame<B> writeInternalResponse(Promise<?> promise, B data) {
        var apiKey = ApiKeys.forId(data.apiKey());
        var header = new ResponseHeaderData();
        int correlationId = 42;
        header.setCorrelationId(correlationId);
        var frame = new InternalResponseFrame<>(filter, promise, apiKey.latestVersion(), correlationId, header, data);
        channel.writeInbound(frame);
        return frame;
    }

    /**
     * Shutdown the channel, asserting there were no further requests or responses to read.
     */
    @AfterEach
    public void assertFinish() {
        boolean finish = channel.finish();
        if (finish) {
            Object inbound = channel.readInbound();
            Object outbound = channel.readOutbound();
            if (inbound != null && outbound != null) {
                fail("Unexpected inbound and outbound messages: inbound: " + inbound + ", outbound: " + outbound);
            }
            else if (inbound != null) {
                fail("Unexpected inbound message: inbound: " + inbound);
            }
            else if (outbound != null) {
                fail("Unexpected outbound message: outbound: " + outbound);
            }
            else {
                fail("Logically this is impossible");
            }
        }
    }
}
