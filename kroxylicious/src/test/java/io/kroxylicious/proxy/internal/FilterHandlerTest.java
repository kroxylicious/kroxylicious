/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.future.Future;
import io.kroxylicious.proxy.future.Promise;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class FilterHandlerTest extends FilterHarness {

    @Test
    public void testForwardRequest() {
        ApiVersionsRequestFilter filter = (request, context) -> context.forwardRequest(request);
        buildChannel(filter);
        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
    }

    @Test
    public void testShouldNotDeserialiseRequest() {
        ApiVersionsRequestFilter filter = new ApiVersionsRequestFilter() {
            @Override
            public boolean shouldDeserializeRequest(ApiKeys apiKey, short apiVersion) {
                return false;
            }

            @Override
            public void onApiVersionsRequest(ApiVersionsRequestData request, KrpcFilterContext context) {
                fail("Should not be called");
            }
        };
        buildChannel(filter);
        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
    }

    @Test
    public void testDropRequest() {
        ApiVersionsRequestFilter filter = (request, context) -> {
            /* don't call forwardRequest => drop the request */ };
        buildChannel(filter);
        var frame = writeRequest(new ApiVersionsRequestData());
    }

    @Test
    public void testForwardResponse() {
        ApiVersionsResponseFilter filter = (response, context) -> context.forwardResponse(response);
        buildChannel(filter);
        var frame = writeResponse(new ApiVersionsResponseData());
        var propagated = channel.readInbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
    }

    @Test
    public void testShouldNotDeserializeResponse() {
        ApiVersionsResponseFilter filter = new ApiVersionsResponseFilter() {
            @Override
            public boolean shouldDeserializeResponse(ApiKeys apiKey, short apiVersion) {
                return false;
            }

            @Override
            public void onApiVersionsResponse(ApiVersionsResponseData response, KrpcFilterContext context) {
                fail("Should not be called");
            }
        };
        buildChannel(filter);
        var frame = writeResponse(new ApiVersionsResponseData());
        var propagated = channel.readInbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
    }

    @Test
    public void testDropResponse() {
        ApiVersionsResponseFilter filter = (response, context) -> {
            /* don't call forwardRequest => drop the request */ };
        buildChannel(filter);
        var frame = writeResponse(new ApiVersionsResponseData());
    }

    @Test
    public void testSendRequest() {
        FetchRequestData body = new FetchRequestData();
        Future<?>[] fut = { null };
        ApiVersionsRequestFilter filter = (request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = context.sendRequest((short) 3, body);
        };

        buildChannel(filter);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        assertFalse(fut[0].isComplete(),
                "Future should not be finished yet");

        // test the response path
        Promise<?> p = (Promise<?>) fut[0];
        var responseFrame = writeInternalResponse(p, new FetchResponseData());
        assertTrue(fut[0].isComplete(),
                "Future should be finished now");
        assertEquals(responseFrame.body(), fut[0].result(),
                "Expect the body that was sent");
    }

    /**
     * Test the special case within {@link FilterHandler} for
     * {@link io.kroxylicious.proxy.filter.KrpcFilterContext#sendRequest(short, ApiMessage)}
     * with acks=0 Produce requests.
     */
    @Test
    public void testSendAcklessProduceRequest() {
        ProduceRequestData body = new ProduceRequestData().setAcks((short) 0);
        Future<?>[] fut = { null };
        ApiVersionsRequestFilter filter = (request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = context.sendRequest((short) 3, body);
        };

        buildChannel(filter);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        assertTrue(fut[0].isComplete(),
                "Future should be done");
        assertTrue(fut[0].succeeded(),
                "Future should be successful");
        assertNull(fut[0].result(),
                "Value should be null");
    }

    @Test
    public void testSendRequestTimeout() throws InterruptedException {
        FetchRequestData body = new FetchRequestData();
        Future<?>[] fut = { null };
        ApiVersionsRequestFilter filter = (request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = context.sendRequest((short) 3, body);
        };

        buildChannel(filter, 50L);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        Future<?> p = (Future<?>) fut[0];
        assertFalse(p.isComplete(),
                "Future should not be finished yet");

        // timeout the request
        Thread.sleep(60L);
        channel.runPendingTasks();

        assertTrue(p.isComplete(),
                "Future should be finished yet");
        assertTrue(p.failed(),
                "Future should be finished yet");
        assertTrue(p.cause() instanceof TimeoutException,
                "Cause should be timeout");

        // check that when the response arrives late, the promise result doesn't change
        var responseFrame = writeInternalResponse((Promise<?>) p, new FetchResponseData());
        assertTrue(p.isComplete(),
                "Future should be finished now");
        assertTrue(p.failed(),
                "Future should be finished yet");
        assertTrue(p.cause() instanceof TimeoutException,
                "Cause should be timeout");
    }

}
