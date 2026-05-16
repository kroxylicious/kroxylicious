/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RouterDispatchHandlerTest {

    private static final int CORRELATION_ID = 42;

    @Mock
    private ClientConnectionStateMachine ccsm;

    private EmbeddedChannel channel;

    @Test
    void shouldDelegateNonFrameMessagesToCcsm() {
        var handler = new RouterDispatchHandler(Map.of(), ccsm);
        channel = new EmbeddedChannel(handler);

        var nonFrame = "not-a-frame";
        channel.writeInbound(nonFrame);

        verify(ccsm).onClientFilterChainComplete(nonFrame);
    }

    @Test
    void shouldThrowForDynamicallyRoutedDecodedFrame() {
        var handler = new RouterDispatchHandler(Map.of(), ccsm);
        channel = new EmbeddedChannel(handler);

        var header = new org.apache.kafka.common.message.RequestHeaderData();
        var body = new FetchRequestData();
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, body);

        assertThatThrownBy(() -> channel.writeInbound(frame))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Dynamic routing is not supported");
    }

    @Test
    void shouldCompletePendingFutureOnResponse() {
        when(ccsm.clientChannel()).thenReturn(null);

        var handler = new RouterDispatchHandler(Map.of(), ccsm);
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
        var handler = new RouterDispatchHandler(Map.of(), ccsm);
        channel = new EmbeddedChannel(handler);
        when(ccsm.clientChannel()).thenReturn(channel);

        var responseHeader = new ResponseHeaderData().setCorrelationId(999);
        var responseBody = new FetchResponseData();
        var responseFrame = new DecodedResponseFrame<>((short) 12, 999, responseHeader, responseBody);

        boolean handled = handler.onResponse(responseFrame);

        assertThat(handled).isFalse();
    }

    @Test
    void shouldForwardStaticallyRoutedDecodedFrameViaForwardToRoute() {
        var staticRoutes = Map.of(ApiKeys.FETCH, "default");
        var handler = new RouterDispatchHandler(staticRoutes, ccsm);
        channel = new EmbeddedChannel(handler);

        var header = new org.apache.kafka.common.message.RequestHeaderData();
        var body = new FetchRequestData();
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, body);

        channel.writeInbound(frame);

        verify(ccsm).forwardToRoute("default", frame);
    }

    @Test
    void shouldForwardStaticallyRoutedOpaqueFrameViaForwardToRoute() {
        var staticRoutes = Map.of(ApiKeys.FETCH, "default");
        var handler = new RouterDispatchHandler(staticRoutes, ccsm);
        channel = new EmbeddedChannel(handler);

        var buf = Unpooled.buffer();
        var opaqueFrame = new OpaqueRequestFrame(
                buf, (short) ApiKeys.FETCH.id, (short) 12, CORRELATION_ID, false, 0, true);

        channel.writeInbound(opaqueFrame);

        verify(ccsm).forwardToRoute("default", opaqueFrame);
        buf.release();
    }

    @Test
    void shouldReturnTrueFromOnResponseWhenPendingFutureExists() {
        when(ccsm.clientChannel()).thenReturn(null);
        var handler = new RouterDispatchHandler(Map.of(), ccsm);
        channel = new EmbeddedChannel(handler);
        when(ccsm.clientChannel()).thenReturn(channel);

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        RouterDispatchHandler.registerPendingResponse(channel, CORRELATION_ID, future);

        var responseHeader = new ResponseHeaderData().setCorrelationId(CORRELATION_ID);
        var responseFrame = new DecodedResponseFrame<>((short) 12, CORRELATION_ID, responseHeader, new FetchResponseData());

        boolean handled = handler.onResponse(responseFrame);

        assertThat(handled).isTrue();
        assertThat(future).isCompleted();
    }

    @Test
    void shouldReturnFalseFromOnResponseWhenNoPendingFuture() {
        var handler = new RouterDispatchHandler(Map.of(), ccsm);
        channel = new EmbeddedChannel(handler);
        when(ccsm.clientChannel()).thenReturn(channel);

        var responseHeader = new ResponseHeaderData().setCorrelationId(999);
        var responseFrame = new DecodedResponseFrame<>((short) 12, 999, responseHeader, new FetchResponseData());

        boolean handled = handler.onResponse(responseFrame);

        assertThat(handled).isFalse();
    }

    @Test
    void shouldRegisterAndRetrievePendingResponses() {
        var handler = new RouterDispatchHandler(Map.of(), ccsm);
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

    @Test
    void shouldFallThroughToCcsmForOpaqueFrameNotInStaticRoutes() {
        var staticRoutes = Map.of(ApiKeys.PRODUCE, "default");
        var handler = new RouterDispatchHandler(staticRoutes, ccsm);
        channel = new EmbeddedChannel(handler);
        when(ccsm.sessionId()).thenReturn("test-session");

        var buf = Unpooled.buffer();
        var opaqueFrame = new OpaqueRequestFrame(
                buf, (short) ApiKeys.FETCH.id, (short) 12, CORRELATION_ID, false, 0, true);

        channel.writeInbound(opaqueFrame);

        verify(ccsm).onClientFilterChainComplete(opaqueFrame);
        buf.release();
    }

    @Test
    void shouldReturnFalseFromOnResponseForNonDecodedFrame() {
        var handler = new RouterDispatchHandler(Map.of(), ccsm);
        channel = new EmbeddedChannel(handler);

        boolean handled = handler.onResponse("not-a-decoded-response-frame");

        assertThat(handled).isFalse();
    }
}
