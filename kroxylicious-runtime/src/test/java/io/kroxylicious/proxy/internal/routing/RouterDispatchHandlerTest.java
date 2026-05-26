/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.router.Response;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RouterDispatchHandlerTest {

    private static final TargetCluster TARGET = new TargetCluster("localhost:9092", Optional.empty());
    private static final int CORRELATION_ID = 42;
    private static final int ROUTING_CORRELATION_ID = Integer.MIN_VALUE / 2;

    @Mock
    private ClientConnectionStateMachine ccsm;

    @Mock
    private Router router;

    private EmbeddedChannel channel;
    private Map<String, RouteDescriptor> routes;
    private SimpleMeterRegistry meterRegistry;
    private AtomicInteger pendingResponseCount;

    @BeforeEach
    void setUp() {
        routes = Map.of("default", new RouteDescriptor("default", 0, TARGET, null, java.util.List.of()));
        meterRegistry = new SimpleMeterRegistry();
        pendingResponseCount = new AtomicInteger();
    }

    private static RouterDispatchHandler.PendingResponse testPendingResponse(CompletableFuture<Response> future) {
        return new RouterDispatchHandler.PendingResponse(
                future, Timer.start(), "default", ApiKeys.FETCH,
                new IdentityNodeIdMapping("default"), body -> {
                });
    }

    private RouterDispatchHandler createHandler(Map<ApiKeys, String> staticRoutes) {
        return new RouterDispatchHandler(
                router, routes, staticRoutes, ccsm,
                new IdentityNodeIdMapping("default"),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount,
                null, null, null);
    }

    private void stubCcsmForRouting() {
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());
    }

    @Test
    void shouldInvokeRouterOnDecodedRequestFrame() {
        stubCcsmForRouting();
        when(router.onRequest(anyShort(), any(ApiKeys.class), any(), any(), any(RouterContext.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);

        var header = new RequestHeaderData();
        var body = new FetchRequestData();
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, body);

        channel.writeInbound(frame);

        verify(router).onRequest(
                anyShort(),
                any(ApiKeys.class),
                any(),
                any(),
                any(RouterContext.class));
    }

    @Test
    void shouldDelegateNonFrameMessagesToCcsm() {
        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);

        var nonFrame = "not-a-frame";
        channel.writeInbound(nonFrame);

        verify(ccsm).onClientFilterChainComplete(nonFrame);
    }

    @Test
    void shouldCloseChannelWhenRouterReturnsFailed() {
        stubCcsmForRouting();
        when(router.onRequest(anyShort(), any(ApiKeys.class), any(), any(), any(RouterContext.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("router error")));

        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);

        var header = new RequestHeaderData();
        var body = new FetchRequestData();
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, body);

        channel.writeInbound(frame);

        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void shouldCompletePendingFutureOnResponse() {
        when(ccsm.clientChannel()).thenReturn(null);
        when(ccsm.sessionId()).thenReturn("test-session");

        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);
        when(ccsm.clientChannel()).thenReturn(channel);

        CompletableFuture<Response> future = new CompletableFuture<>();
        var pending = testPendingResponse(future);
        RouterDispatchHandler.registerPendingResponse(channel, ROUTING_CORRELATION_ID, pending);
        pendingResponseCount.incrementAndGet();

        var responseHeader = new ResponseHeaderData().setCorrelationId(ROUTING_CORRELATION_ID);
        var responseBody = new FetchResponseData();
        var responseFrame = new DecodedResponseFrame<>((short) 12, ROUTING_CORRELATION_ID, responseHeader, responseBody);

        handler.onResponse(responseFrame);

        assertThat(future).isCompleted();
        Response response = future.join();
        assertThat(response.header()).isEqualTo(responseHeader);
        assertThat(response.body()).isEqualTo(responseBody);
        assertThat(pendingResponseCount.get()).isZero();
    }

    @Test
    void shouldNotFailWhenResponseHasNoPendingFuture() {
        when(ccsm.sessionId()).thenReturn("test-session");
        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);
        when(ccsm.clientChannel()).thenReturn(channel);

        int unknownRoutingId = ROUTING_CORRELATION_ID + 999;
        var responseHeader = new ResponseHeaderData().setCorrelationId(unknownRoutingId);
        var responseBody = new FetchResponseData();
        var responseFrame = new DecodedResponseFrame<>((short) 12, unknownRoutingId, responseHeader, responseBody);

        handler.onResponse(responseFrame);
    }

    @Test
    void shouldForwardRequestViaCcsmWhenRouterSendsRequest() {
        stubCcsmForRouting();
        AtomicReference<String> forwardedRoute = new AtomicReference<>();
        AtomicReference<Object> forwardedMsg = new AtomicReference<>();

        doAnswer(invocation -> {
            RouterContext ctx = invocation.getArgument(4);
            int bootstrapId = ctx.bootstrapNodeId("default");
            var reqHeader = new RequestHeaderData()
                    .setRequestApiKey(ApiKeys.FETCH.id)
                    .setRequestApiVersion((short) 12);
            var reqBody = new FetchRequestData();
            ctx.sendRequestToNode("default", bootstrapId, reqHeader, reqBody);
            return CompletableFuture.completedFuture(null);
        }).when(router).onRequest(anyShort(), any(ApiKeys.class), any(), any(), any(RouterContext.class));

        doAnswer(invocation -> {
            forwardedRoute.set(invocation.getArgument(0));
            forwardedMsg.set(invocation.getArgument(1));
            return null;
        }).when(ccsm).forwardToRoute(any(), any());

        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);

        var header = new RequestHeaderData();
        var body = new FetchRequestData();
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, body);

        channel.writeInbound(frame);

        assertThat(forwardedRoute.get()).isEqualTo("default");
        assertThat(forwardedMsg.get()).isInstanceOf(DecodedRequestFrame.class);
    }

    @Test
    void shouldRegisterAndRetrievePendingResponses() {
        when(ccsm.sessionId()).thenReturn("test-session");
        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);

        int routingId1 = ROUTING_CORRELATION_ID;
        int routingId2 = ROUTING_CORRELATION_ID + 1;

        CompletableFuture<Response> future1 = new CompletableFuture<>();
        CompletableFuture<Response> future2 = new CompletableFuture<>();

        RouterDispatchHandler.registerPendingResponse(channel, routingId1,
                testPendingResponse(future1));
        RouterDispatchHandler.registerPendingResponse(channel, routingId2,
                testPendingResponse(future2));

        when(ccsm.clientChannel()).thenReturn(channel);

        var header1 = new ResponseHeaderData().setCorrelationId(routingId1);
        handler.onResponse(new DecodedResponseFrame<>((short) 12, routingId1, header1, new FetchResponseData()));

        assertThat(future1).isCompleted();
        assertThat(future2).isNotCompleted();
    }

    @Test
    void shouldForwardStaticallyRoutedDecodedFrameViaForwardToRoute() {
        when(ccsm.sessionId()).thenReturn("test-session");
        var staticRoutes = Map.of(ApiKeys.FETCH, "default");
        var handler = createHandler(staticRoutes);
        channel = new EmbeddedChannel(handler);

        var header = new RequestHeaderData();
        var body = new FetchRequestData();
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, body);

        channel.writeInbound(frame);

        verify(ccsm).forwardToRoute("default", frame);
        verifyNoInteractions(router);
    }

    @Test
    void shouldForwardStaticallyRoutedOpaqueFrameViaForwardToRoute() {
        when(ccsm.sessionId()).thenReturn("test-session");
        var staticRoutes = Map.of(ApiKeys.FETCH, "default");
        var handler = createHandler(staticRoutes);
        channel = new EmbeddedChannel(handler);

        var buf = Unpooled.buffer();
        var opaqueFrame = new OpaqueRequestFrame(
                buf, (short) ApiKeys.FETCH.id, (short) 12, CORRELATION_ID, false, 0, true);

        channel.writeInbound(opaqueFrame);

        verify(ccsm).forwardToRoute("default", opaqueFrame);
        verifyNoInteractions(router);
        buf.release();
    }

    @Test
    void shouldReturnTrueFromOnResponseWhenPendingFutureExists() {
        when(ccsm.clientChannel()).thenReturn(null);
        when(ccsm.sessionId()).thenReturn("test-session");
        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);
        when(ccsm.clientChannel()).thenReturn(channel);

        CompletableFuture<Response> future = new CompletableFuture<>();
        var pending = testPendingResponse(future);
        RouterDispatchHandler.registerPendingResponse(channel, ROUTING_CORRELATION_ID, pending);

        var responseHeader = new ResponseHeaderData().setCorrelationId(ROUTING_CORRELATION_ID);
        var responseFrame = new DecodedResponseFrame<>((short) 12, ROUTING_CORRELATION_ID, responseHeader, new FetchResponseData());

        boolean handled = handler.onResponse(responseFrame);

        assertThat(handled).isTrue();
        assertThat(future).isCompleted();
    }

    @Test
    void shouldReturnFalseFromOnResponseWhenNoPendingFuture() {
        when(ccsm.sessionId()).thenReturn("test-session");
        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);
        when(ccsm.clientChannel()).thenReturn(channel);

        int unknownRoutingId = ROUTING_CORRELATION_ID + 999;
        var responseHeader = new ResponseHeaderData().setCorrelationId(unknownRoutingId);
        var responseFrame = new DecodedResponseFrame<>((short) 12, unknownRoutingId, responseHeader, new FetchResponseData());

        boolean handled = handler.onResponse(responseFrame);

        assertThat(handled).isFalse();
    }

    @Test
    void shouldIncrementStaticRouteRequestCounter() {
        when(ccsm.sessionId()).thenReturn("test-session");
        var staticRoutes = Map.of(ApiKeys.FETCH, "default");
        var handler = createHandler(staticRoutes);
        channel = new EmbeddedChannel(handler);

        var header = new RequestHeaderData();
        var body = new FetchRequestData();
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, body);

        channel.writeInbound(frame);

        var counter = meterRegistry.find("test_routing_requests").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldCloseRouterOnHandlerRemoved() {
        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);

        channel.pipeline().remove(handler);

        verify(router).close();
    }

    @Test
    void shouldCloseRouterOnChannelClose() {
        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);

        channel.close();

        verify(router).close();
    }

    @Test
    void shouldIncrementErrorCounterWhenRouterFails() {
        stubCcsmForRouting();
        when(router.onRequest(anyShort(), any(ApiKeys.class), any(), any(), any(RouterContext.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));

        var handler = createHandler(Map.of());
        channel = new EmbeddedChannel(handler);

        var header = new RequestHeaderData();
        var body = new FetchRequestData();
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, body);

        channel.writeInbound(frame);

        var counter = meterRegistry.find("test_routing_errors").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }
}
