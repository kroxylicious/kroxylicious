/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.future.BrutalFuture;

import static org.junit.jupiter.api.Assertions.*;

public class FilterHandlerTest extends FilterHarness {

    @Test
    public void testForwardRequest() {
        ApiVersionsRequestFilter filter = (header, request, context) -> context.forwardRequest(request);
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
            public void onApiVersionsRequest(RequestHeaderData header, ApiVersionsRequestData request, KrpcFilterContext context) {
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
        ApiVersionsRequestFilter filter = (header, request, context) -> {
            /* don't call forwardRequest => drop the request */
        };
        buildChannel(filter);
        var frame = writeRequest(new ApiVersionsRequestData());
    }

    @Test
    public void testForwardResponse() {
        ApiVersionsResponseFilter filter = (header, response, context) -> context.forwardResponse(response);
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
            public void onApiVersionsResponse(ResponseHeaderData header, ApiVersionsResponseData response, KrpcFilterContext context) {
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
        ApiVersionsResponseFilter filter = (header, response, context) -> {
            /* don't call forwardRequest => drop the request */
        };
        buildChannel(filter);
        var frame = writeResponse(new ApiVersionsResponseData());
    }

    @Test
    public void testSendRequest() {
        FetchRequestData body = new FetchRequestData();
        BrutalFuture<?>[] fut = { null };
        ApiVersionsRequestFilter filter = (header, request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = (BrutalFuture<?>) context.sendRequest((short) 3, body);
        };

        buildChannel(filter);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        assertFalse(fut[0].isDone(),
                "Future should not be finished yet");

        // test the response path
        BrutalFuture<?> p = fut[0];
        var responseFrame = writeInternalResponse(new FetchResponseData(), p);
        assertTrue(fut[0].isDone(),
                "Future should be finished now");
        assertEquals(responseFrame.body(), fut[0].getNow(null),
                "Expect the body that was sent");
    }

    @Test
    public void testSendRequestCompletionStageCannotBeCompletedByClient() {
        FetchRequestData body = new FetchRequestData();
        CompletableFuture<?>[] fut = { null };
        ApiVersionsRequestFilter filter = (header, request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = context.sendRequest((short) 3, body).toCompletableFuture();
        };

        buildChannel(filter);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        assertFalse(fut[0].isDone(),
                "Future should not be finished yet");

        assertThrows(RuntimeException.class, () -> {
            fut[0].complete(null);
        });
    }

    @Test
    public void testSendRequestCompletionStageCannotBeCompletedExceptionallyByClient() {
        FetchRequestData body = new FetchRequestData();
        CompletableFuture<?>[] fut = { null };
        ApiVersionsRequestFilter filter = (header, request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = context.sendRequest((short) 3, body).toCompletableFuture();
        };

        buildChannel(filter);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        assertFalse(fut[0].isDone(),
                "Future should not be finished yet");

        assertThrows(RuntimeException.class, () -> {
            fut[0].completeExceptionally(new RuntimeException());
        });
    }

    @Test
    public void testCannotBlockingGetOnSendRequestCompletionStage() {
        FetchRequestData body = new FetchRequestData();
        CompletableFuture<?>[] fut = { null };
        ApiVersionsRequestFilter filter = (header, request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = context.sendRequest((short) 3, body).toCompletableFuture();
        };

        buildChannel(filter);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        assertFalse(fut[0].isDone(),
                "Future should not be finished yet");

        assertThrows(RuntimeException.class, () -> {
            fut[0].get();
        });
    }

    /**
     * Test the special case within {@link FilterHandler} for
     * {@link io.kroxylicious.proxy.filter.KrpcFilterContext#sendRequest(short, ApiMessage)}
     * with acks=0 Produce requests.
     */
    @Test
    public void testSendAcklessProduceRequest() throws ExecutionException, InterruptedException {
        ProduceRequestData body = new ProduceRequestData().setAcks((short) 0);
        CompletableFuture<?>[] fut = { null };
        ApiVersionsRequestFilter filter = (header, request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = context.sendRequest((short) 3, body).toCompletableFuture();
        };

        buildChannel(filter);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        assertTrue(fut[0].isDone(),
                "Future should be done");
        assertFalse(fut[0].isCompletedExceptionally(),
                "Future should be successful");
        CompletableFuture<Object> blocking = new CompletableFuture<>();
        fut[0].thenApply(blocking::complete);
        assertNull(blocking.get(),
                "Value should be null");
    }

    @Test
    public void testSendRequestTimeout() throws InterruptedException {
        FetchRequestData body = new FetchRequestData();
        BrutalFuture<?>[] fut = { null };
        ApiVersionsRequestFilter filter = (header, request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = (BrutalFuture<?>) context.sendRequest((short) 3, body);
        };

        buildChannel(filter, 50L);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        BrutalFuture<?> p = (BrutalFuture<?>) fut[0];
        assertFalse(p.isDone(),
                "Future should not be finished yet");

        // timeout the request
        Thread.sleep(60L);
        channel.runPendingTasks();

        assertTrue(p.isDone(),
                "Future should be finished yet");
        assertTrue(p.isCompletedExceptionally(),
                "Future should be finished yet");
        assertTrue(p.exceptionNow() instanceof TimeoutException,
                "Cause should be timeout");

        // check that when the response arrives late, the promise result doesn't change
        var responseFrame = writeInternalResponse(new FetchResponseData(), p);
        assertTrue(p.isDone(),
                "Future should be finished now");
        assertTrue(p.isCompletedExceptionally(),
                "Future should be finished yet");
        assertTrue(p.exceptionNow() instanceof TimeoutException,
                "Cause should be timeout");
    }

}
