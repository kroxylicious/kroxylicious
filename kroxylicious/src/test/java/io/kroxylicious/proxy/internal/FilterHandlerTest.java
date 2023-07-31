/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.future.InternalCompletionStage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class FilterHandlerTest extends FilterHarness {

    public static final int ARBITRARY_TAG = 500;

    @Test
    public void testForwardRequest() {
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.requestFilterResultBuilder().withMessage(request).withHeader(header)
                .completedFilterResult();
        buildChannel(filter);
        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
    }

    @Test
    public void testShouldNotDeserialiseRequest() {
        ApiVersionsRequestFilter filter = new ApiVersionsRequestFilter() {
            @Override
            public boolean shouldHandleApiVersionsRequest(short apiVersion) {
                return false;
            }

            @Override
            public CompletionStage<RequestFilterResult> onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request,
                                                                             KrpcFilterContext context) {
                fail("Should not be called");
                return null;
            }
        };
        buildChannel(filter);
        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
    }

    @Test
    @Disabled
    public void testDropRequest() {
        // FIXME - it only make sense to drop an publish requests where ack=0
        // need to think about how because to single that
        // RequestFilterResult.dropRequest(true) perhaps
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            /* don't call forwardRequest => drop the request */
            return null;
        };
        buildChannel(filter);
        var frame = writeRequest(new ApiVersionsRequestData());
    }

    @Test
    public void testForwardResponse() {
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> context.responseFilterResultBuilder().withHeader(header).withMessage(response)
                .completedFilterResult();
        buildChannel(filter);
        var frame = writeResponse(new ApiVersionsResponseData());
        var propagated = channel.readInbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
    }

    @Test
    public void testOtherFiltersInChainCanFilterOutOfBandResponse() {
        ApiVersionsResponseFilter recipientFilter = taggingApiVersionsResponseFilter("recipient");
        String filterName = "other-interested-filter";
        ApiVersionsResponseFilter filterUnderTest = taggingApiVersionsResponseFilter(filterName);
        buildChannel(filterUnderTest);
        CompletableFuture<Object> future = new CompletableFuture<>();
        var frame = writeInternalResponse(new ApiVersionsResponseData(), future, recipientFilter);
        var propagated = channel.readInbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
        assertResponseMessageTaggedWith(filterName, (InternalResponseFrame<?>) propagated);
        assertFalse(future.isDone());
    }

    @Test
    public void testOtherFiltersInChainCanFilterOutOfBandRequest() {
        ApiVersionsRequestFilter recipientFilter = taggingApiVersionsRequestFilter("recipient");
        String filterName = "other-interested-filter";
        ApiVersionsRequestFilter filterUnderTest = taggingApiVersionsRequestFilter(filterName);
        buildChannel(filterUnderTest);
        CompletableFuture<Object> future = new CompletableFuture<>();
        var frame = writeInternalRequest(new ApiVersionsRequestData(), future, recipientFilter);
        var propagated = channel.readOutbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
        assertRequestMessageTaggedWith(filterName, (DecodedRequestFrame<?>) propagated);
        assertFalse(future.isDone());
    }

    @Test
    public void testShouldNotDeserializeResponse() {
        ApiVersionsResponseFilter filter = new ApiVersionsResponseFilter() {
            @Override
            public boolean shouldHandleApiVersionsResponse(short apiVersion) {
                return false;
            }

            @Override
            public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response,
                                                                               KrpcFilterContext context) {
                fail("Should not be called");
                return null;
            }
        };
        buildChannel(filter);
        var frame = writeResponse(new ApiVersionsResponseData());
        var propagated = channel.readInbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
    }

    @Test
    @Disabled
    public void testDropResponse() {
        // Can't think of a case where dropping a response is every actually
        // legal. I suppose if the filter rewrote a publishrequest (ack 0=>1).
        // then the response filter would need to drop it.

        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> {
            /* don't call forwardRequest => drop the request */
            return null;
        };
        buildChannel(filter);
        var frame = writeResponse(new ApiVersionsResponseData());
    }

    @Test
    public void testSendRequest() {
        FetchRequestData body = new FetchRequestData();
        InternalCompletionStage<ApiMessage>[] fut = new InternalCompletionStage[]{ null };
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = (InternalCompletionStage<ApiMessage>) context.sendRequest((short) 3, body);
            return CompletableFuture.completedStage(null);
        };

        buildChannel(filter);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        InternalCompletionStage<ApiMessage> completionStage = fut[0];
        CompletableFuture<ApiMessage> future = toCompletableFuture(completionStage);
        assertFalse(future.isDone(),
                "Future should not be finished yet");

        // test the response path
        CompletableFuture<ApiMessage> futu = new CompletableFuture<>();
        var responseFrame = writeInternalResponse(new FetchResponseData(), futu);
        assertTrue(futu.isDone(),
                "Future should be finished now");
        assertEquals(responseFrame.body(), futu.getNow(null),
                "Expect the body that was sent");
    }

    private static CompletableFuture<ApiMessage> toCompletableFuture(CompletionStage<ApiMessage> completionStage) {
        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        completionStage.whenComplete((o, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            }
            else {
                future.complete(o);
            }
        });
        return future;
    }

    @Test
    public void testSendRequestCompletionStageCannotBeConvertedToFuture() {
        FetchRequestData body = new FetchRequestData();
        CompletionStage<?>[] fut = { null };
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = context.sendRequest((short) 3, body);
            return CompletableFuture.completedStage(null);
        };

        buildChannel(filter);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        assertThrows(UnsupportedOperationException.class, () -> {
            fut[0].toCompletableFuture();
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
        CompletionStage<ApiMessage>[] fut = new CompletionStage[]{ null };
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = context.sendRequest((short) 3, body);
            return CompletableFuture.completedStage(null);
        };

        buildChannel(filter);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        CompletableFuture<ApiMessage> future = toCompletableFuture(fut[0]);
        assertTrue(future.isDone(),
                "Future should be done");
        assertFalse(future.isCompletedExceptionally(),
                "Future should be successful");
        CompletableFuture<Object> blocking = new CompletableFuture<>();
        fut[0].thenApply(blocking::complete);
        assertNull(blocking.get(),
                "Value should be null");
    }

    @Test
    public void testSendRequestTimeout() throws InterruptedException {
        FetchRequestData body = new FetchRequestData();
        CompletionStage<ApiMessage>[] fut = new CompletionStage[]{ null };
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            assertNull(fut[0],
                    "Expected to only be called once");
            fut[0] = context.sendRequest((short) 3, body);
            return CompletableFuture.completedStage(null);
        };

        buildChannel(filter, 50L);

        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertTrue(propagated instanceof InternalRequestFrame);
        assertEquals(body, ((InternalRequestFrame<?>) propagated).body(),
                "Expect the body to be the Fetch request");

        CompletionStage<ApiMessage> p = fut[0];
        CompletableFuture<ApiMessage> q = toCompletableFuture(p);
        assertFalse(q.isDone(),
                "Future should not be finished yet");

        // timeout the message
        Thread.sleep(60L);
        channel.runPendingTasks();

        assertTrue(q.isDone(),
                "Future should be finished yet");
        assertTrue(q.isCompletedExceptionally(),
                "Future should be finished yet");
        assertThrows(ExecutionException.class, q::get);
    }

    private static void assertResponseMessageTaggedWith(String filterName, DecodedResponseFrame<?> propagated) {
        String tag = collectTagsToStrings(propagated.body(), ARBITRARY_TAG);
        assertEquals(tag, filterName);
    }

    private static void assertRequestMessageTaggedWith(String filterName, DecodedRequestFrame<?> propagated) {
        String tag = collectTagsToStrings(propagated.body(), ARBITRARY_TAG);
        assertEquals(tag, filterName);
    }

    private static ApiVersionsResponseFilter taggingApiVersionsResponseFilter(String tag) {
        return (apiVersion, header, response, context) -> {
            response.unknownTaggedFields().add(new RawTaggedField(ARBITRARY_TAG, tag.getBytes(UTF_8)));
            return context.responseFilterResultBuilder().withHeader(header).withMessage(response).completedFilterResult();
        };
    }

    private static String collectTagsToStrings(ApiMessage body, int tag) {
        return body.unknownTaggedFields().stream().filter(f -> f.tag() == tag)
                .map(RawTaggedField::data).map(f -> new String(f, UTF_8)).collect(Collectors.joining(","));
    }

    private static ApiVersionsRequestFilter taggingApiVersionsRequestFilter(String tag) {
        return (apiVersion, header, request, context) -> {
            request.unknownTaggedFields().add(new RawTaggedField(ARBITRARY_TAG, tag.getBytes(UTF_8)));
            return context.requestFilterResultBuilder().withMessage(request).withHeader(header).completedFilterResult();
        };
    }

}
