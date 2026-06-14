/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RouterDispatchHandlerTest {

    private static final TargetCluster TARGET = new TargetCluster("localhost:9092", Optional.empty());
    private static final int CORRELATION_ID = 42;

    @Mock
    private ClientConnectionStateMachine ccsm;

    @Mock
    private Router router;

    private EmbeddedChannel channel;
    private Map<String, RouteDescriptor> routes;

    @BeforeEach
    void setUp() {
        routes = Map.of("default", new RouteDescriptor("default", 0, TARGET, null, java.util.List.of()));
    }

    private void stubCcsmForRouting() {
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());
    }

    @Test
    void shouldInvokeRouterOnDecodedRequestFrame() {
        stubCcsmForRouting();
        when(router.onRequest(any(ApiKeys.class), anyShort(), any(), any(), any(RouterContext.class)))
                .thenAnswer(inv -> {
                    RouterContext ctx = inv.getArgument(4);
                    return ctx.respondWithoutReply().completed();
                });

        var handler = new RouterDispatchHandler(router, routes, ccsm);
        channel = new EmbeddedChannel(handler);

        var header = new RequestHeaderData();
        var body = new FetchRequestData();
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, body);

        channel.writeInbound(frame);

        verify(router).onRequest(
                any(ApiKeys.class),
                anyShort(),
                any(),
                any(),
                any(RouterContext.class));
    }

    @Test
    void shouldDelegateNonFrameMessagesToCcsm() {
        when(ccsm.sessionId()).thenReturn("test-session");
        var handler = new RouterDispatchHandler(router, routes, ccsm);
        channel = new EmbeddedChannel(handler);

        var nonFrame = "not-a-frame";
        channel.writeInbound(nonFrame);

        verify(ccsm).onClientFilterChainComplete(nonFrame);
    }

    @Test
    void shouldCloseChannelWhenRouterReturnsFailed() {
        stubCcsmForRouting();
        when(router.onRequest(any(ApiKeys.class), anyShort(), any(), any(), any(RouterContext.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("router error")));

        var handler = new RouterDispatchHandler(router, routes, ccsm);
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

        var handler = new RouterDispatchHandler(router, routes, ccsm);
        channel = new EmbeddedChannel(handler);
        when(ccsm.clientChannel()).thenReturn(channel);

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        RouterDispatchHandler.registerPendingResponse(channel, CORRELATION_ID, future);

        var responseHeader = new ResponseHeaderData().setCorrelationId(CORRELATION_ID);
        var responseBody = new FetchResponseData();
        var responseFrame = new DecodedResponseFrame<>((short) 12, CORRELATION_ID, responseHeader, responseBody);

        handler.onResponse(responseFrame);

        assertThat(future).isCompleted();
        ApiMessage response = future.join();
        assertThat(response).isEqualTo(responseBody);
    }

    @Test
    void shouldNotFailWhenResponseHasNoPendingFuture() {
        var handler = new RouterDispatchHandler(router, routes, ccsm);
        channel = new EmbeddedChannel(handler);
        when(ccsm.clientChannel()).thenReturn(channel);

        var responseHeader = new ResponseHeaderData().setCorrelationId(999);
        var responseBody = new FetchResponseData();
        var responseFrame = new DecodedResponseFrame<>((short) 12, 999, responseHeader, responseBody);

        handler.onResponse(responseFrame);
    }

    @Test
    void shouldForwardRequestViaCcsmWhenRouterSendsRequest() {
        stubCcsmForRouting();
        AtomicReference<Object> forwarded = new AtomicReference<>();

        doAnswer(invocation -> {
            RouterContext ctx = invocation.getArgument(4);
            var reqHeader = new RequestHeaderData();
            var reqBody = new FetchRequestData();
            ctx.sendRequest(ctx.anyNode("default"), reqHeader, reqBody);
            return ctx.respondWithoutReply().completed();
        }).when(router).onRequest(any(ApiKeys.class), anyShort(), any(), any(), any(RouterContext.class));

        doAnswer(invocation -> {
            forwarded.set(invocation.getArgument(0));
            return null;
        }).when(ccsm).onClientFilterChainComplete(any());

        var handler = new RouterDispatchHandler(router, routes, ccsm);
        channel = new EmbeddedChannel(handler);

        var header = new RequestHeaderData();
        var body = new FetchRequestData();
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, body);

        channel.writeInbound(frame);

        assertThat(forwarded.get()).isInstanceOf(DecodedRequestFrame.class);
    }

    @Test
    void shouldRegisterAndRetrievePendingResponses() {
        var handler = new RouterDispatchHandler(router, routes, ccsm);
        channel = new EmbeddedChannel(handler);

        CompletableFuture<ApiMessage> future1 = new CompletableFuture<>();
        CompletableFuture<ApiMessage> future2 = new CompletableFuture<>();

        RouterDispatchHandler.registerPendingResponse(channel, 1, future1);
        RouterDispatchHandler.registerPendingResponse(channel, 2, future2);

        when(ccsm.clientChannel()).thenReturn(channel);

        var header1 = new ResponseHeaderData().setCorrelationId(1);
        handler.onResponse(new DecodedResponseFrame<>((short) 12, 1, header1, new FetchResponseData()));

        assertThat(future1).isCompleted();
        assertThat(future2).isNotCompleted();
    }

}
