/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kroxylicious.proxy.internal;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.future.ProxyPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.AfterEach;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * A test harness for {@link KrpcFilter} implementations.
 */
public abstract class FilterHarness {
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
        filterHandler = new FilterHandler(filter, timeoutMs);
        channel = new EmbeddedChannel(filterHandler);
    }

    /**
     * Write a client request to the pipeline.
     * @param data The request body.
     * @return The frame that was sent.
     * @param <B> The type of the request.
     */
    protected <B extends ApiMessage> DecodedRequestFrame<B> writeRequest(B data) {
        //ApiVersionsRequestData data = new ApiVersionsRequestData();
        var apiKey = ApiKeys.forId(data.apiKey());
        var header = new RequestHeaderData();
        int correlationId = 42;
        header.setCorrelationId(correlationId);
        header.setRequestApiKey(apiKey.id);
        header.setRequestApiVersion(apiKey.latestVersion());
        header.setClientId("test-client");
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
        //ApiVersionsRequestData data = new ApiVersionsRequestData();
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
    protected <B extends ApiMessage> DecodedResponseFrame<B> writeInternalResponse(ProxyPromise<?> promise, B data) {
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
            } else if (inbound != null) {
                fail("Unexpected inbound message: inbound: " + inbound);
            } else if (outbound != null) {
                fail("Unexpected outbound message: outbound: " + outbound);
            } else {
                fail("Logically this is impossible");
            }
        }
    }
}
