/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.SslHandler;

import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FetchRequestFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.SaslAuthenticateRequestFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.internal.KafkaAuthnHandler.SaslMechanism;
import io.kroxylicious.proxy.internal.filter.RequestFilterResultBuilderImpl;
import io.kroxylicious.proxy.internal.filter.ResponseFilterResultBuilderImpl;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FilterHandlerTest extends FilterHarness {

    private static final int ARBITRARY_TAG = 500;
    private static final RawTaggedField MARK = createTag(ARBITRARY_TAG, "mark");
    public static final long TIMEOUT_MS = 50L;
    public static final String AUTHORIZATION_ID = "Bob's yer uncle";

    @Test
    void testForwardRequest() {
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.requestFilterResultBuilder().forward(header, request)
                .completed();
        buildChannel(filter);
        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
    }

    // Note: Unpooled.EMPTY_BUFFER is used by KafkaProxyFrontendHandler#closeOnFlush
    @Test
    void canForwardEmptyBuffers() {
        buildChannel();
        channel.writeOutbound(Unpooled.EMPTY_BUFFER);
        var propagated = channel.readOutbound();
        assertThat(propagated).isSameAs(Unpooled.EMPTY_BUFFER);
    }

    @Test
    void testShortCircuitResponse() {
        ApiVersionsResponseData responseData = new ApiVersionsResponseData();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.requestFilterResultBuilder().shortCircuitResponse(responseData)
                .completed();
        buildChannel(filter);
        writeRequest(new ApiVersionsRequestData());
        DecodedResponseFrame<?> propagated = channel.readInbound();
        assertEquals(responseData, propagated.body(), "expected ApiVersionsResponseData to be forwarded");
    }

    // We want to prevent a Kroxylicious filter unexpectedly responding to zero-ack produce requests
    @Test
    void testShortCircuitResponseZeroAcksProduceResponseDropped() {
        ProduceResponseData responseData = new ProduceResponseData();
        ProduceRequestFilter filter = (apiVersion, header, request, context) -> context.requestFilterResultBuilder().shortCircuitResponse(responseData)
                .completed();
        buildChannel(filter);
        writeRequest(new ProduceRequestData().setAcks((short) 0));
        DecodedResponseFrame<?> propagated = channel.readInbound();
        assertNull(propagated, "no message should have been propagated");
    }

    @Test
    void testShortCircuitResponseNonZeroAcksProduceResponseForwarded() {
        ProduceResponseData responseData = new ProduceResponseData();
        ProduceRequestFilter filter = (apiVersion, header, request, context) -> context.requestFilterResultBuilder().shortCircuitResponse(responseData)
                .completed();
        buildChannel(filter);
        writeRequest(new ProduceRequestData().setAcks((short) 1));
        DecodedResponseFrame<?> propagated = channel.readInbound();
        assertEquals(responseData, propagated.body(), "expected ProduceResponseData to be forwarded");
    }

    @Test
    void shortCircuitSendsIncorrectApiResponse() {
        ProduceResponseData responseData = new ProduceResponseData();
        FetchRequestFilter filter = (apiVersion, header, request, context) -> context.requestFilterResultBuilder().shortCircuitResponse(responseData)
                .completed();
        buildChannel(filter);
        writeRequest(new FetchRequestData());

        DecodedResponseFrame<?> propagated = channel.readInbound();
        assertThat(propagated).isNull();
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void deferredRequestMethodsDispatchedOnEventloop() {
        var req1 = new ApiVersionsRequestData().setClientSoftwareName("req1");
        var req2 = new ApiVersionsRequestData().setClientSoftwareName("req2");

        var requestFutureMap = new LinkedHashMap<ApiVersionsRequestData, CompletableFuture<Void>>();
        requestFutureMap.put(req1, new CompletableFuture<>());
        requestFutureMap.put(req2, new CompletableFuture<>());

        // we expect the embedded netty eventloop to run on this thread
        AtomicReference<Thread> expectedDispatchThread = new AtomicReference<>();
        ApiVersionsRequestFilter filter = new ApiVersionsRequestFilter() {

            @Override
            public boolean shouldHandleApiVersionsRequest(short apiVersion) {
                if (Thread.currentThread() != expectedDispatchThread.get()) {
                    throw new IllegalStateException("Filter method dispatched on unexpected thread!");
                }
                return true;
            }

            @Override
            public CompletionStage<RequestFilterResult> onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request,
                                                                             FilterContext context) {
                if (Thread.currentThread() != expectedDispatchThread.get()) {
                    return CompletableFuture.failedFuture(new IllegalStateException("Filter method dispatched on unexpected thread!"));
                }
                // delay required to provoke the second request to be queued
                return requestFutureMap.get(request)
                        .thenCompose(u -> context.forwardRequest(header, request));
            }
        };
        buildChannel(filter);
        expectedDispatchThread.set(obtainEventLoop());
        writeRequest(req1);
        writeRequest(req2);

        // simulate Filter completing from an uncontrolled thread
        CompletableFuture.runAsync(() -> requestFutureMap.get(req1).complete(null)).join();
        channel.runPendingTasks();

        DecodedRequestFrame<?> outboundRequest1 = channel.readOutbound();

        assertThat(outboundRequest1).extracting(DecodedRequestFrame::body).isEqualTo(req1);

        // simulate Filter completing from an uncontrolled thread
        CompletableFuture.runAsync(() -> requestFutureMap.get(req2).complete(null)).join();
        channel.runPendingTasks();

        DecodedRequestFrame<?> outboundRequest2 = channel.readOutbound();

        assertThat(channel.isOpen()).isTrue();
        assertThat(outboundRequest2).extracting(DecodedRequestFrame::body).isEqualTo(req2);
    }

    static Stream<Arguments> deferredRequests() {
        var deferredApiRequest1 = new ApiVersionsRequestData().setClientSoftwareName("req1");
        var deferredApiRequest2 = new ApiVersionsRequestData().setClientSoftwareName("req2");
        var deferredApiRequest3 = new ApiVersionsRequestData().setClientSoftwareName("req3");
        return Stream.of(
                Arguments.argumentSet("single api request deferred",
                        (Consumer<FilterHandlerTest>) test -> {
                            test.writeRequest(deferredApiRequest1);
                        },
                        (Consumer<FilterHandlerTest>) test -> {
                            DecodedRequestFrame<?> outboundRequest = test.channel.readOutbound();
                            assertThat(outboundRequest)
                                    .extracting(DecodedRequestFrame::body)
                                    .isEqualTo(deferredApiRequest1);

                        }),
                Arguments.argumentSet("many deferred api requests received correctly ordered",
                        (Consumer<FilterHandlerTest>) test -> {
                            test.writeRequest(deferredApiRequest1);
                            test.writeRequest(deferredApiRequest2);
                            test.writeRequest(deferredApiRequest3);
                        },
                        (Consumer<FilterHandlerTest>) test -> {
                            List<DecodedRequestFrame<?>> requests = new ArrayList<>();
                            requests.add(test.channel.readOutbound());
                            requests.add(test.channel.readOutbound());
                            requests.add(test.channel.readOutbound());
                            assertThat(requests)
                                    .extracting(DecodedRequestFrame::body)
                                    .asInstanceOf(InstanceOfAssertFactories.list(ApiVersionsRequestData.class))
                                    .containsExactly(deferredApiRequest1, deferredApiRequest2, deferredApiRequest3);

                        }),
                Arguments.argumentSet("many deferred opaque requests received correctly ordered",
                        (Consumer<FilterHandlerTest>) test -> {
                            var resp1 = Unpooled.buffer();
                            resp1.writeByte((byte) 1);
                            var resp2 = Unpooled.buffer();
                            resp2.writeByte((byte) 2);
                            var resp3 = Unpooled.buffer();
                            resp3.writeByte((byte) 3);
                            test.writeArbitraryOpaqueRequest(resp1);
                            test.writeArbitraryOpaqueRequest(resp2);
                            test.writeArbitraryOpaqueRequest(resp3);
                        },
                        (Consumer<FilterHandlerTest>) test -> {
                            List<OpaqueRequestFrame> requests = new ArrayList<>();
                            requests.add(test.channel.readOutbound());
                            requests.add(test.channel.readOutbound());
                            requests.add(test.channel.readOutbound());
                            List<Byte> expected = List.of((byte) 1, (byte) 2, (byte) 3);
                            assertThat(requests)
                                    .extracting(OpaqueRequestFrame::buf)
                                    .extracting(ByteBuf::readByte)
                                    .containsExactlyElementsOf(expected);

                        }),
                Arguments.argumentSet("mixed deferred requests received correctly ordered",
                        (Consumer<FilterHandlerTest>) test -> {
                            test.writeRequest(deferredApiRequest1);
                            var opaqueRequest2 = Unpooled.buffer();
                            opaqueRequest2.writeByte((byte) 2);
                            test.writeArbitraryOpaqueRequest(opaqueRequest2);
                            test.writeRequest(deferredApiRequest3);
                        },
                        (Consumer<FilterHandlerTest>) test -> {
                            DecodedRequestFrame<?> request1 = test.channel.readOutbound();
                            assertThat(request1)
                                    .extracting(DecodedRequestFrame::body)
                                    .isEqualTo(deferredApiRequest1);

                            OpaqueRequestFrame request2 = test.channel.readOutbound();
                            assertThat(request2)
                                    .extracting(OpaqueRequestFrame::buf)
                                    .extracting(ByteBuf::readByte)
                                    .isEqualTo((byte) 2);

                            DecodedRequestFrame<?> request3 = test.channel.readOutbound();
                            assertThat(request3)
                                    .extracting(DecodedRequestFrame::body)
                                    .isEqualTo(deferredApiRequest3);
                        }));
    }

    @ParameterizedTest
    @MethodSource
    void deferredRequests(Consumer<FilterHandlerTest> deferredWork, Consumer<FilterHandlerTest> requestAssertions) {
        var first = new ApiVersionsRequestData().setClientSoftwareName("req0");
        var firstFuture = new CompletableFuture<Void>();

        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> firstFuture.thenCompose(u -> context.forwardRequest(header, request));
        buildChannel(filter);

        writeRequest(first);
        deferredWork.accept(this);
        channel.runPendingTasks();

        // Nothing should be propagated yet, as the first request is awaiting the future
        var propagated = channel.readOutbound();
        assertThat(propagated).isNull();

        // complete first's future, now expect both requests to flow.
        firstFuture.complete(null);
        channel.runPendingTasks();

        DecodedRequestFrame<?> outboundRequest1 = channel.readOutbound();
        assertThat(outboundRequest1).extracting(DecodedRequestFrame::body).isEqualTo(first);

        requestAssertions.accept(this);
    }

    static Stream<Arguments> deferredResponses() {
        var deferredApiResponse1 = new ApiVersionsResponseData().setErrorCode((short) 1);
        var deferredApiResponse2 = new ApiVersionsResponseData().setErrorCode((short) 2);
        var deferredApiResponse3 = new ApiVersionsResponseData().setErrorCode((short) 3);
        return Stream.of(
                Arguments.argumentSet("single api response deferred",
                        (Consumer<FilterHandlerTest>) test -> {
                            test.writeResponse(deferredApiResponse1);
                        },
                        (Consumer<FilterHandlerTest>) test -> {
                            DecodedResponseFrame<?> inboundResponse = test.channel.readInbound();
                            assertThat(inboundResponse)
                                    .extracting(DecodedResponseFrame::body)
                                    .isEqualTo(deferredApiResponse1);

                        }),
                Arguments.argumentSet("many deferred api responses received correctly ordered",
                        (Consumer<FilterHandlerTest>) test -> {
                            test.writeResponse(deferredApiResponse1);
                            test.writeResponse(deferredApiResponse2);
                            test.writeResponse(deferredApiResponse3);
                        },
                        (Consumer<FilterHandlerTest>) test -> {
                            List<DecodedResponseFrame<?>> responses = new ArrayList<>();
                            responses.add(test.channel.readInbound());
                            responses.add(test.channel.readInbound());
                            responses.add(test.channel.readInbound());
                            assertThat(responses)
                                    .extracting(DecodedResponseFrame::body)
                                    .asInstanceOf(InstanceOfAssertFactories.list(ApiVersionsResponseData.class))
                                    .containsExactly(deferredApiResponse1, deferredApiResponse2, deferredApiResponse3);

                        }),
                Arguments.argumentSet("many deferred opaque responses received correctly ordered",
                        (Consumer<FilterHandlerTest>) test -> {
                            var resp1 = Unpooled.buffer();
                            resp1.writeByte((byte) 1);
                            var resp2 = Unpooled.buffer();
                            resp2.writeByte((byte) 2);
                            var resp3 = Unpooled.buffer();
                            resp3.writeByte((byte) 3);
                            test.writeArbitraryOpaqueResponse(resp1);
                            test.writeArbitraryOpaqueResponse(resp2);
                            test.writeArbitraryOpaqueResponse(resp3);
                        },
                        (Consumer<FilterHandlerTest>) test -> {
                            List<OpaqueResponseFrame> responses = new ArrayList<>();
                            responses.add(test.channel.readInbound());
                            responses.add(test.channel.readInbound());
                            responses.add(test.channel.readInbound());
                            List<Byte> expected = List.of((byte) 1, (byte) 2, (byte) 3);
                            assertThat(responses)
                                    .extracting(OpaqueResponseFrame::buf)
                                    .extracting(ByteBuf::readByte)
                                    .containsExactlyElementsOf(expected);

                        }),
                Arguments.argumentSet("mixed deferred responses received correctly ordered",
                        (Consumer<FilterHandlerTest>) test -> {
                            test.writeResponse(deferredApiResponse1);
                            var opaqueResponse2 = Unpooled.buffer();
                            opaqueResponse2.writeByte((byte) 2);
                            test.writeArbitraryOpaqueResponse(opaqueResponse2);
                            test.writeResponse(deferredApiResponse3);
                        },
                        (Consumer<FilterHandlerTest>) test -> {
                            DecodedResponseFrame<?> response1 = test.channel.readInbound();
                            assertThat(response1)
                                    .extracting(DecodedResponseFrame::body)
                                    .isEqualTo(deferredApiResponse1);

                            OpaqueResponseFrame response2 = test.channel.readInbound();
                            assertThat(response2)
                                    .extracting(OpaqueResponseFrame::buf)
                                    .extracting(ByteBuf::readByte)
                                    .isEqualTo((byte) 2);

                            DecodedResponseFrame<?> response3 = test.channel.readInbound();
                            assertThat(response3)
                                    .extracting(DecodedResponseFrame::body)
                                    .isEqualTo(deferredApiResponse3);
                        }));
    }

    @ParameterizedTest
    @MethodSource
    void deferredResponses(Consumer<FilterHandlerTest> deferredWork, Consumer<FilterHandlerTest> responseAssertions) {
        var first = new ApiVersionsResponseData().setErrorCode((short) -1);
        var firstFuture = new CompletableFuture<Void>();

        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> firstFuture.thenCompose(u -> context.forwardResponse(header, response));
        buildChannel(filter);

        writeResponse(first);
        deferredWork.accept(this);

        channel.runPendingTasks();

        // Nothing should be propagated yet, as the first response is awaiting the future
        var propagated = channel.readInbound();
        assertThat(propagated).isNull();

        // complete first's future, now expect both response to flow.
        firstFuture.complete(null);
        channel.runPendingTasks();

        DecodedResponseFrame<?> inboundResponse1 = channel.readInbound();
        assertThat(inboundResponse1).extracting(DecodedResponseFrame::body).isEqualTo(first);

        responseAssertions.accept(this);
    }

    @Test
    void testDeferredRequestTimeout() {
        var filterFuture = new CompletableFuture<RequestFilterResult>();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> filterFuture;
        timeout(TIMEOUT_MS).buildChannel(filter);
        channel.freezeTime();
        writeRequest(new ApiVersionsRequestData());
        channel.advanceTimeBy(TIMEOUT_MS - 1, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        assertThat(filterFuture).isNotDone();
        channel.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();

        assertThat(filterFuture).isCompletedExceptionally().isNotCancelled();
        assertThatThrownBy(filterFuture::get).hasCauseInstanceOf(TimeoutException.class);
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void testDeferredResponseTimeout() {
        var filterFuture = new CompletableFuture<ResponseFilterResult>();
        ApiVersionsResponseFilter filter = (apiVersion, header, request, context) -> filterFuture;
        timeout(TIMEOUT_MS).buildChannel(filter);
        channel.freezeTime();
        writeResponse(new ApiVersionsResponseData());
        channel.advanceTimeBy(TIMEOUT_MS - 1, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        assertThat(filterFuture).isNotDone();
        channel.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();

        assertThat(filterFuture).isCompletedExceptionally().isNotCancelled();
        assertThatThrownBy(filterFuture::get).hasCauseInstanceOf(TimeoutException.class);
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void testUserResponseFilterReturnsNullFuture() {
        ApiVersionsResponseFilter filter = (apiVersion, header, request, context) -> null;
        timeout(TIMEOUT_MS).buildChannel(filter);
        writeResponse(new ApiVersionsResponseData());
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void testUserResponseFilterReturnsEmptyFuture() {
        CompletableFuture<ResponseFilterResult> filterFuture = CompletableFuture.completedFuture(null);
        ApiVersionsResponseFilter filter = (apiVersion, header, request, context) -> filterFuture;
        timeout(TIMEOUT_MS).buildChannel(filter);
        writeResponse(new ApiVersionsResponseData());
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void testUserRequestFilterReturnsNullFuture() {
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> null;
        timeout(TIMEOUT_MS).buildChannel(filter);
        writeRequest(new ApiVersionsRequestData());
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void testUserRequestFilterReturnsEmptyFuture() {
        CompletableFuture<RequestFilterResult> filterFuture = CompletableFuture.completedFuture(null);
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> filterFuture;
        timeout(TIMEOUT_MS).buildChannel(filter);
        writeRequest(new ApiVersionsRequestData());
        assertThat(channel.isOpen()).isFalse();
    }

    static Stream<Arguments> requestFilterClosesChannel() {
        return Stream.of(
                Arguments.of("completes exceptionally",
                        (BiFunction<RequestHeaderData, ApiMessage, CompletionStage<RequestFilterResult>>) (header, request) -> CompletableFuture
                                .failedStage(new RuntimeException("filter error")),
                        false),
                Arguments.of("filter result signals close",
                        (BiFunction<RequestHeaderData, ApiMessage, CompletionStage<RequestFilterResult>>) (header, request) -> new RequestFilterResultBuilderImpl()
                                .withCloseConnection().completed(),
                        false),
                Arguments.of("filter result signals close with forward",
                        (BiFunction<RequestHeaderData, ApiMessage, CompletionStage<RequestFilterResult>>) (header, request) -> new RequestFilterResultBuilderImpl()
                                .forward(header, request).withCloseConnection().completed(),
                        true));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void requestFilterClosesChannel(String name,
                                    BiFunction<RequestHeaderData, ApiMessage, CompletableFuture<RequestFilterResult>> stageFunction,
                                    boolean forwardExpected) {
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> stageFunction.apply(header, request);
        buildChannel(filter);
        var frame = writeRequest(new ApiVersionsRequestData());
        channel.runPendingTasks();

        assertThat(channel.isOpen()).isFalse();
        var propagated = channel.readOutbound();
        if (forwardExpected) {
            assertThat(propagated).isEqualTo(frame);
        }
        else {
            assertThat(propagated).isNull();
        }
    }

    static Stream<Arguments> responseFilterClosesChannel() {
        return Stream.of(
                Arguments.of("completes exceptionally",
                        (BiFunction<ResponseHeaderData, ApiMessage, CompletionStage<ResponseFilterResult>>) (header, response) -> CompletableFuture
                                .failedStage(new RuntimeException("filter error")),
                        false),
                Arguments.of("filter result signals close",
                        (BiFunction<ResponseHeaderData, ApiMessage, CompletionStage<ResponseFilterResult>>) (header, response) -> new ResponseFilterResultBuilderImpl()
                                .withCloseConnection().completed(),
                        false),
                Arguments.of("filter result signals close with forward",
                        (BiFunction<ResponseHeaderData, ApiMessage, CompletionStage<ResponseFilterResult>>) (header, response) -> new ResponseFilterResultBuilderImpl()
                                .forward(header, response).withCloseConnection().completed(),
                        true));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void responseFilterClosesChannel(String name,
                                     BiFunction<ResponseHeaderData, ApiMessage, CompletableFuture<ResponseFilterResult>> stageFunction,
                                     boolean forwardExpected) {
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> stageFunction.apply(header, response);
        buildChannel(filter);
        var frame = writeResponse(new ApiVersionsResponseData());
        channel.runPendingTasks();

        assertThat(channel.isOpen()).isFalse();
        var propagated = channel.readInbound();
        if (forwardExpected) {
            assertThat(propagated).isEqualTo(frame);
        }
        else {
            assertThat(propagated).isNull();
        }
    }

    @Test
    void closedChannelIgnoresDeferredPendingRequests() {
        var seen = new ArrayList<ApiMessage>();
        var filterFuture = new CompletableFuture<RequestFilterResult>();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            seen.add(request);
            return filterFuture;
        };
        buildChannel(filter);
        var frame1 = writeRequest(new ApiVersionsRequestData());
        writeRequest(new ApiVersionsRequestData().setClientSoftwareName("should not be processed"));
        // the filter handler will have queued up the second request, awaiting the completion of the first.
        filterFuture.complete(new RequestFilterResultBuilderImpl().withCloseConnection().build());
        channel.runPendingTasks();

        assertThat(channel.isOpen()).isFalse();
        var propagated = channel.readOutbound();
        assertThat(propagated).isNull();
        assertThat(seen).containsExactly(frame1.body());
    }

    @Test
    void closedChannelReleasesOpaqueDeferredPendingRequests() {
        var seen = new ArrayList<ApiMessage>();
        var filterFuture = new CompletableFuture<RequestFilterResult>();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            seen.add(request);
            return filterFuture;
        };
        buildChannel(filter);
        var first = writeRequest(new ApiVersionsRequestData());
        var opaqueBuf = spy(Unpooled.buffer());
        writeArbitraryOpaqueRequest(opaqueBuf);
        // the filter handler will have queued up the opaque request, awaiting the completion of the first.
        filterFuture.complete(new RequestFilterResultBuilderImpl().withCloseConnection().build());
        channel.runPendingTasks();

        assertThat(channel.isOpen()).isFalse();
        var propagated = channel.readOutbound();
        assertThat(propagated).isNull();
        assertThat(seen).containsExactly(first.body());

        verify(opaqueBuf).release();
    }

    @Test
    void closedChannelIgnoresDeferredPendingResponse() {
        var seen = new ArrayList<ApiMessage>();
        var filterFuture = new CompletableFuture<ResponseFilterResult>();
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> {
            seen.add(response);
            return filterFuture;
        };
        buildChannel(filter);
        var frame1 = writeResponse(new ApiVersionsResponseData().setErrorCode((short) 1));
        writeResponse(new ApiVersionsResponseData().setErrorCode((short) 2));
        // the filter handler will have queued up the second response, awaiting the completion of the first.
        filterFuture.complete(new ResponseFilterResultBuilderImpl().withCloseConnection().build());
        channel.runPendingTasks();

        assertThat(channel.isOpen()).isFalse();
        var propagated = channel.readInbound();
        assertThat(propagated).isNull();
        assertThat(seen).containsExactly(frame1.body());
    }

    @Test
    void closedChannelReleasesOpaqueDeferredPendingResponses() {
        var seen = new ArrayList<ApiMessage>();
        var filterFuture = new CompletableFuture<ResponseFilterResult>();
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> {
            seen.add(response);
            return filterFuture;
        };
        buildChannel(filter);
        var frame1 = writeResponse(new ApiVersionsResponseData().setErrorCode((short) 1));
        var opaqueBuf = spy(Unpooled.buffer());
        writeArbitraryOpaqueResponse(opaqueBuf);

        writeResponse(new ApiVersionsResponseData().setErrorCode((short) 2));
        // the filter handler will have queued up the second response, awaiting the completion of the first.
        filterFuture.complete(new ResponseFilterResultBuilderImpl().withCloseConnection().build());
        channel.runPendingTasks();

        assertThat(channel.isOpen()).isFalse();
        var propagated = channel.readInbound();
        assertThat(propagated).isNull();
        assertThat(seen).containsExactly(frame1.body());

        verify(opaqueBuf).release();
    }

    @Test
    void testShouldNotDeserialiseRequest() {
        ApiVersionsRequestFilter filter = new ApiVersionsRequestFilter() {
            @Override
            public boolean shouldHandleApiVersionsRequest(short apiVersion) {
                return false;
            }

            @Override
            public CompletionStage<RequestFilterResult> onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request,
                                                                             FilterContext context) {
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
    void testDropRequest() {
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            /* don't call forwardRequest => drop the request */
            return context.requestFilterResultBuilder().drop().completed();
        };
        buildChannel(filter);
        var frame = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertNull(propagated);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void requestShortCircuit(boolean withClose) {
        var shortCircuitResponse = new ApiVersionsResponseData();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            var builder = context.requestFilterResultBuilder()
                    .shortCircuitResponse(shortCircuitResponse);

            if (withClose) {
                builder.withCloseConnection();
            }
            return builder.completed();
        };
        buildChannel(filter);
        writeRequest(new ApiVersionsRequestData());

        assertThat(channel.isOpen()).isEqualTo(!withClose);

        var propagatedOutbound = channel.readOutbound();
        assertThat(propagatedOutbound).isNull();

        var propagatedInbound = channel.readInbound();
        assertThat(propagatedInbound).isNotNull();
        assertThat(((DecodedResponseFrame<?>) propagatedInbound).body()).isEqualTo(shortCircuitResponse);
    }

    @Test
    void testForwardResponse() {
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> context.forwardResponse(header, response);
        buildChannel(filter);
        var frame = writeResponse(new ApiVersionsResponseData());
        var propagated = channel.readInbound();
        assertEquals(frame, propagated, "Expect it to be the frame that was sent");
    }

    @Test
    void testShouldNotDeserializeResponse() {
        ApiVersionsResponseFilter filter = new ApiVersionsResponseFilter() {
            @Override
            public boolean shouldHandleApiVersionsResponse(short apiVersion) {
                return false;
            }

            @Override
            public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response,
                                                                               FilterContext context) {
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
    void testDropResponse() {
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> {
            return context.responseFilterResultBuilder().drop().completed();
        };
        buildChannel(filter);
        var frame = writeResponse(new ApiVersionsResponseData());
        var propagated = channel.readInbound();
        assertNull(propagated);

    }

    /**
     * Tests that a filter is capable of marking an out-of-band (oob) request and the
     * result is delayed until the out-of-band response is received.
     */
    @Test
    void sendRequest() {
        var oobRequestBody = new FetchRequestData();
        var oobHeader = new RequestHeaderData().setRequestApiVersion(FetchRequestData.LOWEST_SUPPORTED_VERSION);
        var snoopedOobRequestResponseStage = new AtomicReference<CompletionStage<FetchResponseData>>();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            assertNull(snoopedOobRequestResponseStage.get(), "Expected to only be called once");
            snoopedOobRequestResponseStage.set(context.sendRequest(oobHeader, oobRequestBody));
            return snoopedOobRequestResponseStage.get()
                    .thenCompose(u -> context.forwardRequest(header, request));
        };

        buildChannel(filter);

        // trigger filter
        var requestFrame = writeRequest(new ApiVersionsRequestData());

        // verify filter has sent the send request.
        InternalRequestFrame<?> propagatedOobRequest = channel.readOutbound();
        assertThat(propagatedOobRequest.body()).isEqualTo(oobRequestBody);
        assertThat(propagatedOobRequest.header()).isNotNull();

        // verify oob request response future is in the expected state
        assertThat(snoopedOobRequestResponseStage).isNotNull();
        var snoopedOobRequestResponseFuture = toCompletableFuture(snoopedOobRequestResponseStage.get());
        assertThat(snoopedOobRequestResponseFuture).withFailMessage("out-of-band request response future was expected to be pending but it is done.").isNotDone();

        // mimic the broker sending the oob response
        var responseFrame = writeInternalResponse(propagatedOobRequest.header().correlationId(), new FetchResponseData());
        assertThat(snoopedOobRequestResponseFuture).isCompletedWithValueMatching(r -> Objects.equals(r, responseFrame.body()));

        // verify the filter has forwarded the request showing the that OOB request future completed.
        var propagated = channel.readOutbound();
        assertThat(propagated).isEqualTo(requestFrame);
    }

    @Test
    void shouldTimeoutSendRequest() {
        var oobRequestBody = new FetchRequestData();
        var oobHeader = new RequestHeaderData().setRequestApiVersion(FetchRequestData.LOWEST_SUPPORTED_VERSION);
        var snoopedOobRequestResponseStage = new AtomicReference<CompletionStage<FetchResponseData>>();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            assertNull(snoopedOobRequestResponseStage.get(), "Expected to only be called once");
            snoopedOobRequestResponseStage.set(context.sendRequest(oobHeader, oobRequestBody));
            return snoopedOobRequestResponseStage.get()
                    .thenCompose(u -> context.forwardRequest(header, request));
        };

        timeout(TIMEOUT_MS).buildChannel(filter);

        // trigger filter
        writeRequest(new ApiVersionsRequestData());
        channel.readOutbound();

        var snoopedOobRequestResponseFuture = toCompletableFuture(snoopedOobRequestResponseStage.get());

        // When
        channel.advanceTimeBy(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();

        // Then
        assertThat(snoopedOobRequestResponseFuture).isDone().isCompletedExceptionally();
    }

    static Stream<Arguments> sendRequestRejectsNulls() {
        return Stream.of(
                Arguments.of(new RequestHeaderData(), null),
                Arguments.of(null, new FetchRequestData()));
    }

    @ParameterizedTest
    @MethodSource
    void sendRequestRejectsNulls(RequestHeaderData oobRequestHeader, FetchRequestData oobRequest) {
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            assertThatThrownBy(() -> {
                context.sendRequest(oobRequestHeader, oobRequest);
            }).isInstanceOf(NullPointerException.class);
            return null;
        };

        buildChannel(filter);

        // trigger filter
        writeRequest(new ApiVersionsRequestData());

        // verify filter has not sent the send request.
        InternalRequestFrame<?> propagatedOobRequest = channel.readOutbound();
        assertThat(propagatedOobRequest).isNull();

        // verify that the filter has propagated nothing
        var propagated = channel.readInbound();
        assertThat(propagated).isNull();
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void sendRequestRejectsRequestVersionThatIsOutOfRange() {
        var oobRequest = new FetchRequestData();
        RequestHeaderData oobRequestHeader = new RequestHeaderData().setRequestApiVersion((short) (oobRequest.highestSupportedVersion() + 1));
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            assertThatThrownBy(() -> {
                context.sendRequest(oobRequestHeader, oobRequest);
            }).isInstanceOf(IllegalArgumentException.class);
            return null;
        };

        buildChannel(filter);

        // trigger filter
        writeRequest(new ApiVersionsRequestData());

        // verify filter has not sent the send request.
        InternalRequestFrame<?> propagatedOobRequest = channel.readOutbound();
        assertThat(propagatedOobRequest).isNull();

        // verify that the filter has propagated nothing
        var propagated = channel.readInbound();
        assertThat(propagated).isNull();
        assertThat(channel.isOpen()).isFalse();
    }

    static Stream<Arguments> sendRequestHeaderHandling() {
        ApiMessageType fetch = ApiMessageType.FETCH;
        return Stream.of(
                Arguments.of("api key set",
                        (Supplier<RequestHeaderData>) () -> new RequestHeaderData().setRequestApiVersion(fetch.lowestSupportedVersion()),
                        (Consumer<RequestHeaderData>) (h) -> assertThat(h.requestApiKey()).isEqualTo(fetch.apiKey())),
                Arguments.of("clientid",
                        (Supplier<RequestHeaderData>) () -> new RequestHeaderData().setClientId("clientid").setRequestApiVersion(fetch.lowestSupportedVersion()),
                        (Consumer<RequestHeaderData>) (h) -> assertThat(h.clientId()).isEqualTo("clientid")),
                Arguments.of("version",
                        (Supplier<RequestHeaderData>) () -> new RequestHeaderData().setRequestApiVersion(fetch.highestSupportedVersion(false)),
                        (Consumer<RequestHeaderData>) (h) -> assertThat(h.requestApiVersion()).isEqualTo(fetch.highestSupportedVersion(false))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void sendRequestHeaderHandling(String name, Supplier<RequestHeaderData> headerSupplier, Consumer<RequestHeaderData> headerConsumer) {
        var oobRequestBody = new FetchRequestData();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.sendRequest(headerSupplier.get(), oobRequestBody)
                .thenCompose(u -> context.requestFilterResultBuilder().drop().completed());

        buildChannel(filter);

        // trigger filter
        writeRequest(new ApiVersionsRequestData());

        // verify the header
        InternalRequestFrame<?> propagatedOobRequest = channel.readOutbound();
        assertThat(propagatedOobRequest.header()).isNotNull();
        headerConsumer.accept(propagatedOobRequest.header());

        // mimic the broker sending the oob response
        writeInternalResponse(propagatedOobRequest.header().correlationId(), new FetchResponseData());

        var propagated = channel.readOutbound();
        assertThat(propagated).isNull();
    }

    @Test
    void sendRequestCompletionStageCannotBeConvertedToFuture() {
        var oobRequestBody = new FetchRequestData();
        var oobHeader = new RequestHeaderData().setRequestApiVersion(FetchRequestData.LOWEST_SUPPORTED_VERSION);
        var snoopedOobRequestResponseStage = new AtomicReference<CompletionStage<FetchResponseData>>();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            snoopedOobRequestResponseStage.set(context.sendRequest(oobHeader, oobRequestBody));
            // TODO - it'd be a better test if the filter made the call to toCompletableFuture and the filter failed.
            // We'd then assert that the filter had closed the connection for the right reason. However we currently
            // don't have a way to trap the exception that causes a filter to close.
            return context.requestFilterResultBuilder().drop().completed();
        };

        buildChannel(filter);

        // trigger filter
        writeRequest(new ApiVersionsRequestData());

        // verify filter has sent the send request.
        InternalRequestFrame<?> propagatedAsyncRequest = channel.readOutbound();
        assertThat(propagatedAsyncRequest.body()).isEqualTo(oobRequestBody);

        // verify async request response future is in the expected state
        assertThat(snoopedOobRequestResponseStage).doesNotHaveValue(null);

        var apiMessageCompletionStage = snoopedOobRequestResponseStage.get();
        assertThatThrownBy(apiMessageCompletionStage::toCompletableFuture)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("CompletableFuture usage disallowed");
    }

    /**
     * Test the special case within {@link FilterHandler} for
     * {@link FilterContext#sendRequest(RequestHeaderData, ApiMessage)}
     * with acks=0 Produce requests.
     */
    @Test
    void sendAcklessProduceRequest() {
        var oobRequestBody = new ProduceRequestData().setAcks((short) 0);
        var oobRequestHeader = new RequestHeaderData().setRequestApiVersion(ProduceRequestData.LOWEST_SUPPORTED_VERSION);
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.sendRequest(oobRequestHeader, oobRequestBody)
                .thenCompose(u -> context.forwardRequest(header, request));

        buildChannel(filter);

        // trigger filter
        var requestFrame = writeRequest(new ApiVersionsRequestData());

        // verify filter has sent the send request.
        InternalRequestFrame<?> propagatedAsyncRequest = channel.readOutbound();
        assertThat(propagatedAsyncRequest.body()).isEqualTo(oobRequestBody);

        // verify the filter has forwarded the request showing the that OOB request future completed.
        var propagated = channel.readOutbound();
        assertThat(propagated).isEqualTo(requestFrame);
    }

    @Test
    void sendRequestTimeout() {
        var oobRequestBody = new FetchRequestData();
        var oobHeader = new RequestHeaderData().setRequestApiVersion(FetchRequestData.LOWEST_SUPPORTED_VERSION);
        var snoopedOobRequestResponseStage = new AtomicReference<CompletionStage<FetchResponseData>>();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            snoopedOobRequestResponseStage.set(context.sendRequest(oobHeader, oobRequestBody));
            return context.requestFilterResultBuilder().drop().completed();
        };

        timeout(TIMEOUT_MS).buildChannel(filter);
        channel.freezeTime();

        // trigger filter
        writeRequest(new ApiVersionsRequestData());

        // verify filter has sent the send request.
        InternalRequestFrame<?> propagatedAsyncRequest = channel.readOutbound();
        assertThat(propagatedAsyncRequest.body()).isEqualTo(oobRequestBody);

        // verify async request response future is in the expected state
        assertThat(snoopedOobRequestResponseStage).isNotNull();
        var snoopedOobRequestResponseFuture = toCompletableFuture(snoopedOobRequestResponseStage.get());
        assertThat(snoopedOobRequestResponseFuture).withFailMessage("out-of-band request response future in wrong state").isNotDone();

        // advance to 1ms before timeout
        channel.advanceTimeBy(49, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        assertThat(snoopedOobRequestResponseFuture).withFailMessage("out-of-band request response future in wrong state").isNotDone();

        // advance to timeout
        channel.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();

        assertThat(snoopedOobRequestResponseFuture).withFailMessage("Future should be finished").isCompletedExceptionally();
        assertThatThrownBy(() -> snoopedOobRequestResponseFuture.get()).hasCauseInstanceOf(TimeoutException.class).hasMessageContaining("failed to complete within");
    }

    @Test
    void sendRequestChainedActionsRunOnNettyEventLoop() {

        var oobRequestBody = new FetchRequestData();
        var oobHeader = new RequestHeaderData().setRequestApiVersion(FetchRequestData.LOWEST_SUPPORTED_VERSION);
        var applyActionThread = new AtomicReference<Thread>();
        var applyAsyncActionThread = new AtomicReference<Thread>();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.sendRequest(oobHeader, oobRequestBody)
                .thenApply(u1 -> {
                    applyActionThread.set(Thread.currentThread());
                    return null;
                }).thenApplyAsync(u2 -> {
                    applyAsyncActionThread.set(Thread.currentThread());
                    return null;
                }).thenCompose(u3 -> context.forwardRequest(header, request));

        buildChannel(filter);

        // trigger filter
        var requestFrame = writeRequest(new ApiVersionsRequestData());

        // capture the thread used by the embedded channel
        Thread eventloopThread = obtainEventLoop();

        // verify filter has sent the send request.
        InternalRequestFrame<?> propagatedOobRequest = channel.readOutbound();
        assertThat(propagatedOobRequest.body()).isEqualTo(oobRequestBody);

        // mimic the broker sending the response
        writeInternalResponse(propagatedOobRequest.header().correlationId(), new FetchResponseData());

        // Running the tasks will run the actions chained to the async response
        channel.runPendingTasks();

        // Verify actions ran on the expected thread.
        assertThat(applyActionThread)
                .describedAs("first chained action (apply) must run on event loop")
                .hasValue(eventloopThread);

        assertThat(applyAsyncActionThread)
                .describedAs("second chained action (applySync) must run on event loop")
                .hasValue(eventloopThread);

        // Verify the filtered request arrived at outcome.
        var propagated = channel.readOutbound();
        assertThat(propagated).isEqualTo(requestFrame);
    }

    private Thread obtainEventLoop() {
        var eventLoopThreadFuture = new CompletableFuture<Thread>();
        channel.eventLoop().submit(() -> eventLoopThreadFuture.complete(Thread.currentThread()));
        channel.runPendingTasks();
        assertThat(eventLoopThreadFuture).isCompleted();
        return eventLoopThreadFuture.getNow(null);
    }

    @Test
    void sendMultipleRequests() {
        var firstRequestBody = new FetchRequestData();
        var firstRequestHeader = new RequestHeaderData().setRequestApiVersion(FetchRequestData.LOWEST_SUPPORTED_VERSION);
        var secondRequestBody = new MetadataRequestData();
        var snoopedFirstRequestResponseStage = new AtomicReference<CompletionStage<FetchResponseData>>();
        var snoopedSecondRequestResponseStage = new AtomicReference<CompletionStage<MetadataResponseData>>();
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            assertNull(snoopedFirstRequestResponseStage.get(), "Expected to only be called once");
            snoopedFirstRequestResponseStage.set(context.sendRequest(firstRequestHeader, firstRequestBody));
            return snoopedFirstRequestResponseStage.get()
                    .thenCompose(u -> {
                        assertNull(snoopedSecondRequestResponseStage.get(), "Expected to only be called once");
                        snoopedSecondRequestResponseStage.set(context.sendRequest(firstRequestHeader, secondRequestBody));
                        return snoopedSecondRequestResponseStage.get();
                    })
                    .thenComposeAsync(u -> context.forwardRequest(header, request));
        };

        buildChannel(filter);

        // trigger filter
        var requestFrame = writeRequest(new ApiVersionsRequestData());

        // verify filter has sent the send first request.
        InternalRequestFrame<?> propagatedFirstRequest = channel.readOutbound();
        assertThat(propagatedFirstRequest.body()).isEqualTo(firstRequestBody);
        assertThat(propagatedFirstRequest.header()).isNotNull();

        // verify first request response future is in the expected state
        assertThat(snoopedFirstRequestResponseStage).isNotNull();
        var snoopedFirstRequestResponseFuture = toCompletableFuture(snoopedFirstRequestResponseStage.get());
        assertThat(snoopedFirstRequestResponseFuture).withFailMessage("out-of-band request response future was expected to be pending but it is done.").isNotDone();

        // mimic the broker sending the first response
        var firstResponseFrame = writeInternalResponse(propagatedFirstRequest.header().correlationId(), new FetchResponseData());
        assertThat(snoopedFirstRequestResponseFuture).isCompletedWithValueMatching(r -> Objects.equals(r, firstResponseFrame.body()));

        // verify filter has sent the send second request.
        InternalRequestFrame<?> propagatedSecondRequest = channel.readOutbound();
        assertThat(propagatedSecondRequest.body()).isEqualTo(secondRequestBody);
        assertThat(propagatedSecondRequest.header()).isNotNull();

        // verify second request response future is in the expected state
        assertThat(snoopedSecondRequestResponseStage).isNotNull();
        var snoopedSecondRequestResponseFuture = toCompletableFuture(snoopedSecondRequestResponseStage.get());
        assertThat(snoopedSecondRequestResponseFuture).withFailMessage("out-of-band request response future was expected to be pending but it is done.").isNotDone();

        // mimic the broker sending the second response
        var secondResponseFrame = writeInternalResponse(propagatedSecondRequest.header().correlationId(), new MetadataResponseData());
        assertThat(snoopedSecondRequestResponseFuture).isCompletedWithValueMatching(r -> Objects.equals(r, secondResponseFrame.body()));

        // verify the filter has forwarded the request showing the that OOB request future completed.
        var propagated = channel.readOutbound();
        assertThat(propagated).isEqualTo(requestFrame);
    }

    @Test
    void upstreamFiltersCanFilterOutOfBandRequest() {
        var oobRequestBody = new FetchRequestData();
        var oobHeader = new RequestHeaderData().setRequestApiVersion(FetchRequestData.LOWEST_SUPPORTED_VERSION);
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.sendRequest(oobHeader, oobRequestBody)
                .thenCompose(outOfBandResponse -> context.requestFilterResultBuilder().drop().completed());

        // this filter will intercept the out-of-band request and add the mark
        FetchRequestFilter markingFilter = (apiVersion, header, request, context) -> {
            request.unknownTaggedFields().add(MARK);
            return context.forwardRequest(header, request);
        };

        buildChannel(filter, markingFilter);

        // trigger first filter
        writeRequest(new ApiVersionsRequestData());

        // verify filter has sent the out-of-band request.
        InternalRequestFrame<?> propagatedOobRequest = channel.readOutbound();
        assertThat(propagatedOobRequest.body()).isEqualTo(oobRequestBody);
        // and ensure that it carries the expected mark added by the intercepting filter
        assertThat(propagatedOobRequest.body().unknownTaggedFields()).containsExactly(MARK);
    }

    @Test
    void upstreamFiltersCanFilterOutOfBandResponse() {
        var oobRequestBody = new FetchRequestData();
        var oobHeader = new RequestHeaderData().setRequestApiVersion(FetchRequestData.LOWEST_SUPPORTED_VERSION);
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.sendRequest(oobHeader, oobRequestBody)
                .thenCompose(outOfBandResponse -> {
                    assertThat(outOfBandResponse.unknownTaggedFields()).containsExactly(MARK);
                    return context.forwardRequest(header, request);
                });

        // this filter will intercept the response to the out-of-band request and add the mark
        FetchResponseFilter markingFilter = (apiVersion, header, response, context) -> {
            response.unknownTaggedFields().add(MARK);
            return context.forwardResponse(header, response);
        };

        buildChannel(filter, markingFilter);

        // trigger first filter
        var requestFrame = writeRequest(new ApiVersionsRequestData());

        // verify filter has sent the out-of-band request.
        InternalRequestFrame<?> propagatedOobRequest = channel.readOutbound();
        assertThat(propagatedOobRequest.body()).isEqualTo(oobRequestBody);

        // mimic the broker sending the out-of-band response
        writeInternalResponse(propagatedOobRequest.header().correlationId(), new FetchResponseData());
        channel.runPendingTasks();

        // Verify the filtered response arrived at inbound.
        var propagated = channel.readOutbound();
        assertThat(propagated).isEqualTo(requestFrame);
    }

    private static RawTaggedField createTag(int arbitraryTag, String data) {
        return new RawTaggedField(arbitraryTag, data.getBytes(UTF_8));
    }

    private static <T> CompletableFuture<T> toCompletableFuture(CompletionStage<T> completionStage) {
        var future = new CompletableFuture<T>();
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

    static List<Arguments> clientTlsContext() throws SSLPeerUnverifiedException {

        X509Certificate proxyCertificate = mock(X509Certificate.class);
        X509Certificate proxyIssuer = mock(X509Certificate.class);
        X509Certificate clientCertificate = mock(X509Certificate.class);
        X509Certificate clientIssuer = mock(X509Certificate.class);
        return List.of(
                Arguments.argumentSet("mTLS self-signed",
                        getSslHandler(List.of(proxyCertificate), false, List.of(clientCertificate)),
                        clientCertificate, proxyCertificate),
                Arguments.argumentSet("mTLS chains",
                        getSslHandler(List.of(proxyCertificate, proxyIssuer), false, List.of(clientCertificate, clientIssuer)),
                        clientCertificate, proxyCertificate),
                Arguments.argumentSet("null client cert",
                        getSslHandler(List.of(proxyCertificate), false, null),
                        null, proxyCertificate),
                Arguments.argumentSet("empty client cert",
                        getSslHandler(List.of(proxyCertificate), false, List.of()),
                        null, proxyCertificate),
                Arguments.argumentSet("peer unverified",
                        getSslHandler(List.of(proxyCertificate), true, null),
                        null, proxyCertificate),
                Arguments.argumentSet("No TLS",
                        null, null, null),
                Arguments.argumentSet("No TLS (empty local certs)",
                        getSslHandler(List.of(), true, null),
                        null, null),
                Arguments.argumentSet("No TLS (empty local certs)",
                        getSslHandler(null, true, null),
                        null, null));

    }

    @NonNull
    private static SslHandler getSslHandler(@Nullable List<X509Certificate> proxyCertificates,
                                            boolean peerUnverified,
                                            @Nullable List<X509Certificate> clientCertificates)
            throws SSLPeerUnverifiedException {
        var session = mock(SSLSession.class);
        when(session.getLocalCertificates()).thenReturn(proxyCertificates != null ? proxyCertificates.toArray(new Certificate[0]) : null);
        if (peerUnverified) {
            if (clientCertificates != null) {
                throw new IllegalStateException();
            }
            when(session.getPeerCertificates()).thenThrow(new SSLPeerUnverifiedException(""));
        }
        else {
            when(session.getPeerCertificates()).thenReturn(clientCertificates != null ? clientCertificates.toArray(new Certificate[0]) : null);
        }
        var engine = mock(SSLEngine.class);
        Mockito.when(engine.getSession()).thenReturn(session);
        var handler = mock(SslHandler.class);
        Mockito.when(handler.engine()).thenReturn(engine);
        return handler;
    }

    @ParameterizedTest
    @MethodSource
    void clientTlsContext(@Nullable SslHandler handler,
                          @Nullable X509Certificate clientCertificate,
                          @Nullable X509Certificate proxyCertificate) {

        // when
        var localCert = FilterHandler.localTlsCertificate(handler);
        var peerCert = FilterHandler.getPeerTlsCertificate(handler);

        // then
        assertThat(localCert).isSameAs(proxyCertificate);
        assertThat(peerCert).isSameAs(clientCertificate);
    }

    @Test
    void shouldUpdateClientSaslContextOnSaslAuthSuccess() {
        // Given
        SaslAuthenticateResponseData responseData = new SaslAuthenticateResponseData().setSessionLifetimeMs(10_000);
        buildChannel((SaslAuthenticateRequestFilter) (apiVersion, header, request, context) -> {
            context.clientSaslAuthenticationSuccess(SaslMechanism.SCRAM_SHA_512.name(), AUTHORIZATION_ID);
            return context.requestFilterResultBuilder().shortCircuitResponse(responseData).completed();
        });

        // When
        writeRequest(new SaslAuthenticateRequestData().setAuthBytes("Let me IN!".getBytes(UTF_8)));

        // Then
        DecodedResponseFrame<?> propagated = channel.readInbound();
        assertThat(propagated.body()).isEqualTo(responseData);

        assertThat(clientSaslManager.clientSaslContext())
                .isNotEmpty()
                .hasValueSatisfying(saslContext -> {
                    assertThat(saslContext.authorizationId()).isEqualTo(AUTHORIZATION_ID);
                    assertThat(saslContext.mechanismName()).isEqualTo(SaslMechanism.SCRAM_SHA_512.name());
                });
    }

    @Test
    void shouldUpdateClientSaslContextOnSaslAuthFailure() {
        // Given
        SaslAuthenticateResponseData responseData = new SaslAuthenticateResponseData().setErrorMessage("the doors are closed");
        buildChannel((SaslAuthenticateRequestFilter) (apiVersion, header, request, context) -> {
            context.clientSaslAuthenticationFailure(SaslMechanism.SCRAM_SHA_512.name(), null, new SaslAuthenticationException("the doors re closed"));
            return context.requestFilterResultBuilder().shortCircuitResponse(responseData).completed();
        });

        // When
        writeRequest(new SaslAuthenticateRequestData().setAuthBytes("Let me IN!".getBytes(UTF_8)));

        // Then
        DecodedResponseFrame<?> propagated = channel.readInbound();
        assertThat(propagated.body()).isEqualTo(responseData);

        assertThat(clientSaslManager.clientSaslContext())
                .isEmpty();
    }
}
