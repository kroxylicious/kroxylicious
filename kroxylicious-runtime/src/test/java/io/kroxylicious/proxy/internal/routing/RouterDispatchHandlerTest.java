/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.InternalRequestFrame;
import io.kroxylicious.proxy.internal.InternalResponseFrame;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RouterDispatchHandlerTest {

    private static final int CORRELATION_ID = 42;
    private static final String DEFAULT_ROUTE = "default";

    @Mock
    private Router router;

    @Mock
    private ClientConnectionStateMachine ccsm;

    @Mock
    private Filter filter;

    private EmbeddedChannel channel;

    private RouterDispatchHandler handlerWithIdentityMapping(Map<ApiKeys, String> staticRoutes) {
        return new RouterDispatchHandler(
                router, Map.of(), staticRoutes, ccsm, "test-cluster", new IdentityNodeIdMapping(DEFAULT_ROUTE), null);
    }

    private EmbeddedChannel channelWithTerminal(RouterDispatchHandler handler) {
        return new EmbeddedChannel(handler, new RoutingTerminalHandler(ccsm));
    }

    @Test
    void shouldForwardNonFrameMessageToCcsm() {
        // Given
        var handler = handlerWithIdentityMapping(Map.of());
        channel = channelWithTerminal(handler);

        // When
        channel.writeInbound("not-a-frame");

        // Then
        verify(ccsm).onClientFilterChainComplete("not-a-frame");
    }

    @Test
    void shouldDispatchDynamicallyForDecodedFrame() {
        // Given
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true,
                new RequestHeaderData(), new FetchRequestData());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.authenticatedSubject()).thenReturn(io.kroxylicious.proxy.authentication.Subject.anonymous());

        var handler = handlerWithIdentityMapping(Map.of());
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(frame);

        // Then: router.onRequest was called with the frame's API key and version
        verify(router).onRequest(any(ApiKeys.class), anyShort(), any(), any(), any());
    }

    @Test
    void shouldForwardOpaqueFrameNotInStaticRoutesToCcsm() {
        // Given
        var staticRoutes = Map.of(ApiKeys.PRODUCE, DEFAULT_ROUTE);
        var handler = handlerWithIdentityMapping(staticRoutes);
        channel = channelWithTerminal(handler);

        var buf = Unpooled.buffer();
        var opaqueFrame = new OpaqueRequestFrame(buf, ApiKeys.FETCH.id, (short) 12, CORRELATION_ID, false, 0, true);

        // When
        channel.writeInbound(opaqueFrame);

        // Then
        verify(ccsm).onClientFilterChainComplete(opaqueFrame);
        buf.release();
    }

    @Test
    void shouldForwardStaticallyRoutedDecodedFrameViaForwardToRoute() {
        // Given
        var staticRoutes = Map.of(ApiKeys.FETCH, DEFAULT_ROUTE);
        var handler = handlerWithIdentityMapping(staticRoutes);
        channel = channelWithTerminal(handler);

        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true,
                new RequestHeaderData(), new FetchRequestData());

        // When
        channel.writeInbound(frame);

        // Then
        verify(ccsm).forwardToRoute(DEFAULT_ROUTE, frame);
        assertThat(frame.routeName()).isEqualTo(DEFAULT_ROUTE);
    }

    @Test
    void shouldForwardStaticallyRoutedOpaqueFrameViaForwardToRoute() {
        // Given
        var staticRoutes = Map.of(ApiKeys.FETCH, DEFAULT_ROUTE);
        var handler = handlerWithIdentityMapping(staticRoutes);
        channel = channelWithTerminal(handler);

        var buf = Unpooled.buffer();
        var opaqueFrame = new OpaqueRequestFrame(buf, ApiKeys.FETCH.id, (short) 12, CORRELATION_ID, false, 0, true);

        // When
        channel.writeInbound(opaqueFrame);

        // Then
        verify(ccsm).forwardToRoute(DEFAULT_ROUTE, opaqueFrame);
        assertThat(opaqueFrame.routeName()).isEqualTo(DEFAULT_ROUTE);
        buf.release();
    }

    @Test
    void shouldTranslateNodeIdsInMetadataResponse() {
        // Given: bijective mapping with two routes; METADATA statically routed to route-a
        var mapping = new BijectiveNodeIdMapping(Map.of("route-a", 0, "route-b", 1), 2);
        var handler = new RouterDispatchHandler(
                router, Map.of(), Map.of(ApiKeys.METADATA, "route-a"), ccsm, "test-cluster", mapping, null);
        channel = channelWithTerminal(handler);

        // Record the pending METADATA request
        var requestFrame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true,
                new RequestHeaderData(), new MetadataRequestData());
        channel.writeInbound(requestFrame);

        // When: METADATA response arrives with upstream node IDs 0 and 1
        var md = new MetadataResponseData();
        md.setControllerId(0);
        md.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(0).setHost("h0").setPort(9092));
        md.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(1).setHost("h1").setPort(9093));
        var responseFrame = new DecodedResponseFrame<>((short) 12, CORRELATION_ID, new ResponseHeaderData(), md);
        channel.writeOutbound(responseFrame);

        // Then: the outbound frame has translated node IDs
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        var translatedMd = (MetadataResponseData) out.body();
        // route-a has id=0, totalRoutes=2: virtual(0,0)=0, virtual(0,1)=2
        assertThat(translatedMd.brokers().find(0)).isNotNull(); // node 0 → virtual 0
        assertThat(translatedMd.brokers().find(2)).isNotNull(); // node 1 → virtual 2
        assertThat(translatedMd.controllerId()).isZero(); // virtual 0
    }

    // Helpers for dynamic dispatch tests

    private RouterDispatchHandler handlerWithRoute(String routeName) {
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());
        var rd = new RouteDescriptor(routeName, 0, new TargetCluster("localhost:9092", null), null, List.of());
        return new RouterDispatchHandler(
                router, Map.of(routeName, rd), Map.of(), ccsm, "test-cluster", new IdentityNodeIdMapping(routeName), null);
    }

    private RouterDispatchHandler handlerWithRouteForSendTests(String routeName) {
        var rd = new RouteDescriptor(routeName, 0, new TargetCluster("localhost:9092", null), null, List.of());
        return new RouterDispatchHandler(
                router, Map.of(routeName, rd), Map.of(), ccsm, "test-cluster", new IdentityNodeIdMapping(routeName), null);
    }

    private DecodedRequestFrame<ProduceRequestData> produceFrame(int correlationId) {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9)
                .setCorrelationId(correlationId);
        return new DecodedRequestFrame<>((short) 9, correlationId, true, header, new ProduceRequestData());
    }

    @Test
    void shouldSetClientCorrelationIdOnRespondWithResponse() {
        // Given
        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);
        var body = new MetadataRequestData();
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, body, false)));

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then: the response frame's wire correlation ID matches the client's
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.header().correlationId())
                .as("header.correlationId() must match the client's correlation ID so the wire format is correct")
                .isEqualTo(CORRELATION_ID);
    }

    @Test
    void shouldSetClientCorrelationIdOnRespondWithExplicitHeader() {
        // Given: router provides its own header (which has a different correlationId)
        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);
        var routerHeader = new ResponseHeaderData().setCorrelationId(999);
        var body = new MetadataRequestData();
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(routerHeader, body, false)));

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then: the client's correlation ID overwrites whatever the router put in the header
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.header().correlationId()).isEqualTo(CORRELATION_ID);
    }

    @Test
    void shouldSetClientCorrelationIdOnRespondWithErrorResponse() {
        // Given
        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);
        var requestHeader = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9)
                .setCorrelationId(CORRELATION_ID);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithError(
                                requestHeader, new ProduceRequestData().setAcks((short) 1),
                                new UnknownServerException("test"), false)));

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then: the error response frame's wire correlation ID matches the client's
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.header().correlationId()).isEqualTo(CORRELATION_ID);
    }

    @Test
    void shouldPassThroughUnknownCorrelationIdInResponse() {
        // Given: a handler with no pending requests
        var handler = handlerWithIdentityMapping(Map.of(ApiKeys.METADATA, DEFAULT_ROUTE));
        channel = new EmbeddedChannel(handler);

        // When: a response arrives for an unknown correlation ID
        var md = new MetadataResponseData().setControllerId(5);
        var responseFrame = new DecodedResponseFrame<>((short) 12, 9999, new ResponseHeaderData(), md);
        channel.writeOutbound(responseFrame);

        // Then: it passes through untranslated
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(((MetadataResponseData) out.body()).controllerId()).isEqualTo(5);
    }

    @Test
    void shouldCloseChannelWhenRouterReturnedFutureFails() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));
        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void shouldCloseChannelWhenRouterReturnsNullResult() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void shouldCloseChannelAfterRespondWithWhenCloseConnectionIsTrue() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, new MetadataRequestData(), true)));
        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void shouldCallOnRoutedRequestCompleteAfterDynamicDispatch() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));
        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        verify(ccsm).onRoutedRequestComplete();
    }

    @Test
    void shouldNotWriteOutboundFrameForRespondWithoutReply() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));
        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then: no frame written to client
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    void respondWithoutReplyShouldNotBlockSubsequentResponse() {
        // Given: first request gets no reply, second gets a response
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new RouterResponseImpl.RespondWithoutReply(false)))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, new MetadataResponseData(), false)));
        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.writeInbound(produceFrame(CORRELATION_ID + 1));
        channel.runPendingTasks();

        // Then: second request's response is delivered; no response for the first
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.correlationId()).isEqualTo(CORRELATION_ID + 1);
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    void shouldCloseRouterWhenHandlerRemoved() {
        // Given
        var handler = handlerWithIdentityMapping(Map.of());
        channel = new EmbeddedChannel(handler);

        // When
        channel.pipeline().remove(handler);

        // Then
        verify(router).close();
    }

    @Test
    void writeShouldPassThroughNonFrame() {
        // Given
        var handler = handlerWithIdentityMapping(Map.of());
        channel = new EmbeddedChannel(handler);

        // When / Then
        channel.writeOutbound("not-a-frame");
        assertThat((Object) channel.readOutbound()).isEqualTo("not-a-frame");
    }

    @Test
    void writeShouldPassThroughFrameWithNonRoutingCorrelationId() {
        // Given
        var handler = handlerWithIdentityMapping(Map.of());
        channel = new EmbeddedChannel(handler);
        var frame = new DecodedResponseFrame<>((short) 9, 99, new ResponseHeaderData(), new ProduceResponseData());

        // When
        channel.writeOutbound(frame);

        // Then
        assertThat((Object) channel.readOutbound()).isSameAs(frame);
    }

    @Test
    void writeShouldInterceptRoutingResponseAndCompleteItsFuture() {
        // Given
        when(ccsm.sessionId()).thenReturn("test-session");
        var handler = new RouterDispatchHandler(
                router, Map.of(DEFAULT_ROUTE, new RouteDescriptor(DEFAULT_ROUTE, 0, new TargetCluster("localhost:9092", null), null, List.of())),
                Map.of(), ccsm, "test-cluster", new IdentityNodeIdMapping(DEFAULT_ROUTE), null);
        channel = channelWithTerminal(handler);

        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9);
        var pendingFuture = handler.sendToAnyNode(DEFAULT_ROUTE, header, new ProduceRequestData().setAcks((short) 1), "test-session", 100)
                .toCompletableFuture();

        int routingCorrelationId = Integer.MIN_VALUE / 2;
        var responseFrame = new DecodedResponseFrame<>((short) 9, routingCorrelationId,
                new ResponseHeaderData(), new ProduceResponseData());

        // When
        channel.writeOutbound(responseFrame);

        // Then: frame was consumed by the handler, not forwarded to the client
        assertThat((Object) channel.readOutbound()).isNull();
        assertThat(pendingFuture).isCompletedWithValueMatching(ProduceResponseData.class::isInstance);
    }

    @Test
    void metadataRoutingResponseShouldPopulateSharedNodeAddressMap() {
        // Given
        when(ccsm.sessionId()).thenReturn("test-session");
        var sharedAddresses = new HashMap<Integer, HostPort>();
        var handler = new RouterDispatchHandler(
                router,
                Map.of(DEFAULT_ROUTE, new RouteDescriptor(DEFAULT_ROUTE, 0, new TargetCluster("localhost:9092", null), null, List.of())),
                Map.of(), sharedAddresses, ccsm, "test-cluster", new IdentityNodeIdMapping(DEFAULT_ROUTE), null);
        channel = channelWithTerminal(handler);

        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.METADATA.id)
                .setRequestApiVersion((short) 12);
        handler.sendToAnyNode(DEFAULT_ROUTE, header, new MetadataRequestData(), "test-session", 100);

        int routingCorrelationId = Integer.MIN_VALUE / 2;
        var broker = new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(0).setHost("broker0").setPort(9092);
        var md = new MetadataResponseData();
        md.brokers().add(broker);
        var responseFrame = new DecodedResponseFrame<>((short) 12, routingCorrelationId, new ResponseHeaderData(), md);

        // When
        channel.writeOutbound(responseFrame);

        // Then: shared map contains the address learned from the METADATA response
        assertThat(handler.resolveRouterNodeAddress(0))
                .isPresent()
                .hasValueSatisfying(hp -> {
                    assertThat(hp.host()).isEqualTo("broker0");
                    assertThat(hp.port()).isEqualTo(9092);
                });
        assertThat(sharedAddresses).containsKey(0);
    }

    @Test
    void writeShouldCloseChannelForUnmatchedRoutingCorrelationId() {
        // Given
        when(ccsm.sessionId()).thenReturn("test-session");
        var handler = new RouterDispatchHandler(
                router, Map.of(DEFAULT_ROUTE, new RouteDescriptor(DEFAULT_ROUTE, 0, new TargetCluster("localhost:9092", null), null, List.of())),
                Map.of(), ccsm, "test-cluster", new IdentityNodeIdMapping(DEFAULT_ROUTE), null);
        channel = channelWithTerminal(handler);

        int routingCorrelationId = Integer.MIN_VALUE / 2;
        var frame = new DecodedResponseFrame<>((short) 9, routingCorrelationId,
                new ResponseHeaderData(), new ProduceResponseData());

        // When: no pending response registered
        channel.writeOutbound(frame);

        // Then: frame was consumed (not forwarded) and the channel was closed
        assertThat((Object) channel.readOutbound()).isNull();
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void sendToAnyNodeShouldForwardToRouteForKnownRoute() {
        // Given
        var handler = handlerWithRouteForSendTests(DEFAULT_ROUTE);
        channel = channelWithTerminal(handler);
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12);

        // When
        var future = handler.sendToAnyNode(DEFAULT_ROUTE, header, new FetchRequestData(), "test-session", 100);

        // Then
        verify(ccsm).forwardToRoute(eq(DEFAULT_ROUTE), any());
        assertThat(future.toCompletableFuture()).isNotDone();
    }

    @Test
    void sendToAnyNodeShouldReturnFailedFutureForUnknownRoute() {
        // Given
        var handler = handlerWithRouteForSendTests(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12);

        // When: bypass anyNode() validation by constructing VirtualNodeImpl with an unknown route
        var future = handler.sendToAnyNode("no-such-route", header, new FetchRequestData(), "test-session", 100);

        // Then
        assertThat(future.toCompletableFuture()).isCompletedExceptionally();
        assertThatThrownBy(() -> future.toCompletableFuture().get())
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .cause().hasMessageContaining("Unknown route");
    }

    @Test
    void sendToAnyNodeShouldReturnFailedFutureForNestedRouterRoute() {
        // Given: routes include a route targeting a nested router (no targetCluster)
        var rd = new RouteDescriptor(DEFAULT_ROUTE, 0, new TargetCluster("localhost:9092", null), null, List.of());
        var routerRd = new RouteDescriptor("router-route", 1, null, "some-router-name", List.of());
        var handler = new RouterDispatchHandler(
                router, Map.of(DEFAULT_ROUTE, rd, "router-route", routerRd),
                Map.of(), ccsm, "test-cluster", new IdentityNodeIdMapping(DEFAULT_ROUTE), null);
        channel = new EmbeddedChannel(handler);
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12);

        // When
        var future = handler.sendToAnyNode("router-route", header, new FetchRequestData(), "test-session", 100);

        // Then
        assertThat(future.toCompletableFuture()).isCompletedExceptionally();
        assertThatThrownBy(() -> future.toCompletableFuture().get())
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void sendToAnyNodeShouldReturnCompletedNullFutureForFireAndForget() {
        // Given: PRODUCE with acks=0 has hasResponse()=false, so no response is expected
        var handler = handlerWithRouteForSendTests(DEFAULT_ROUTE);
        channel = channelWithTerminal(handler);
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9);

        // When
        var future = handler.sendToAnyNode(DEFAULT_ROUTE, header, new ProduceRequestData().setAcks((short) 0), "test-session", 100);

        // Then
        verify(ccsm).forwardToRoute(eq(DEFAULT_ROUTE), any());
        assertThat(future.toCompletableFuture()).isCompletedWithValue(null);
    }

    @Test
    void sendToSpecificNodeShouldForwardToNode() {
        // Given
        var handler = handlerWithRouteForSendTests(DEFAULT_ROUTE);
        channel = channelWithTerminal(handler);
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12);

        // When
        var future = handler.sendToSpecificNode(3, DEFAULT_ROUTE, header, new FetchRequestData(), "test-session", 100);

        // Then
        verify(ccsm).forwardToNode(eq(3), eq(DEFAULT_ROUTE), any());
        assertThat(future.toCompletableFuture()).isNotDone();
    }

    @Test
    void sendToSpecificNodeShouldReturnFailedFutureForUnknownRoute() {
        // Given
        var handler = handlerWithRouteForSendTests(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12);

        // When
        var future = handler.sendToSpecificNode(3, "no-such-route", header, new FetchRequestData(), "test-session", 100);

        // Then
        assertThat(future.toCompletableFuture()).isCompletedExceptionally();
        assertThatThrownBy(() -> future.toCompletableFuture().get())
                .hasCauseInstanceOf(IllegalStateException.class)
                .cause().hasMessageContaining("resolved to invalid route");
    }

    @Test
    void sendToSpecificNodeShouldNotRegisterPendingResponseForFireAndForgetRequest() {
        // Given: PRODUCE with acks=0 has hasResponse()=false, so no response is expected
        var handler = handlerWithRouteForSendTests(DEFAULT_ROUTE);
        channel = channelWithTerminal(handler);
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9);

        // When
        var future = handler.sendToSpecificNode(0, DEFAULT_ROUTE, header, new ProduceRequestData().setAcks((short) 0), "test-session", 100);

        // Then: request forwarded, no pending entry left, future already complete
        verify(ccsm).forwardToNode(eq(0), eq(DEFAULT_ROUTE), any());
        assertThat(handler.pendingResponses).isEmpty();
        assertThat(future.toCompletableFuture()).isCompletedWithValue(null);
    }

    @Test
    void pendingFuturesCompletedExceptionallyWhenConnectionCloses() {
        // Given
        var handler = handlerWithRouteForSendTests(DEFAULT_ROUTE);
        channel = channelWithTerminal(handler);
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12);
        var future = handler.sendToAnyNode(DEFAULT_ROUTE, header, new FetchRequestData(), "test-session", 100);
        assertThat(future.toCompletableFuture()).isNotDone();

        // When
        channel.close().syncUninterruptibly();

        // Then
        assertThat(future.toCompletableFuture()).isCompletedExceptionally();
        assertThat(handler.pendingResponses).isEmpty();
    }

    @Test
    void oobFrameRespondWithErrorShouldCompletePromiseExceptionally() {
        // Given: OOB whose router returns RespondWithError — sequence slot was already skipped for OOB;
        // deliverResponse must NOT call responseSequencer.submit() with the already-skipped slot.
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise);
        oob.setRouteName(DEFAULT_ROUTE);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithError(new RequestHeaderData(), new ProduceRequestData(), new UnknownServerException("oob-error"), false)));
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then: OOB promise must be completed exceptionally — not orphaned in the sequencer
        assertThat(promise).isCompletedExceptionally();
    }

    @Test
    void oobFrameRespondWithoutReplyShouldCompletePromise() {
        // Given: OOB whose router returns RespondWithoutReply — same orphan risk as RespondWithError.
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise);
        oob.setRouteName(DEFAULT_ROUTE);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new RouterResponseImpl.RespondWithoutReply(false)));
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then: OOB promise must be completed — not orphaned in the sequencer
        assertThat(promise).isDone();
    }

    @Test
    void oobFrameRespondWithShouldDeliverInternalResponseFrame() {
        // Given
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise);
        oob.setRouteName(DEFAULT_ROUTE);
        var body = new ProduceResponseData();
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new RouterResponseImpl.RespondWith(null, body, false)));
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then: InternalResponseFrame delivered directly (not a plain DecodedResponseFrame)
        var out = channel.readOutbound();
        assertThat(out).isInstanceOf(InternalResponseFrame.class);
        InternalResponseFrame<?> irf = (InternalResponseFrame<?>) out;
        assertThat(irf.correlationId()).isEqualTo(CORRELATION_ID);
        assertThat(irf.routeName()).isEqualTo(DEFAULT_ROUTE);
        assertThat(irf.promise()).isSameAs(promise);
    }

    @Test
    void oobFrameShouldSkipSequenceImmediately() {
        // Given: first request is an OOB whose router future is held pending; second is a regular request
        var pendingOobFuture = new CompletableFuture<RouterResponse>();
        var oob = oobProduceFrame(CORRELATION_ID, new CompletableFuture<>());
        var regularFrame = produceFrame(CORRELATION_ID + 1);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(pendingOobFuture.minimalCompletionStage())
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, new ProduceResponseData(), false)));
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When: OOB is sent first (pending), then a regular frame is sent
        channel.writeInbound(oob);
        channel.writeInbound(regularFrame);
        channel.runPendingTasks();

        // Then: regular frame's response arrives immediately — not blocked by the pending OOB sequence
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out)
                .as("regular frame response must not be blocked by the pending OOB sequence slot")
                .isNotNull();
        assertThat(out.correlationId()).isEqualTo(CORRELATION_ID + 1);
    }

    @Test
    void oobFrameErrorShouldCloseChannelWithoutDoubleSkip() {
        // Given
        var oob = oobProduceFrame(CORRELATION_ID, new CompletableFuture<>());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then: channel is closed; no exception (sequence not skipped twice)
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void oobFrameNullResultShouldCloseChannelAndCompletePromiseExceptionally() {
        // Given
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise);
        oob.setRouteName(DEFAULT_ROUTE);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then
        assertThat(channel.isOpen()).isFalse();
        assertThat(promise).isCompletedExceptionally();
    }

    @Test
    void oobFrameUnrecognisedResultTypeShouldCloseChannelAndCompletePromiseExceptionally() {
        // Given
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise);
        oob.setRouteName(DEFAULT_ROUTE);
        RouterResponse unknown = new RouterResponse() {
        };
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(unknown));
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then
        assertThat(channel.isOpen()).isFalse();
        assertThat(promise).isCompletedExceptionally();
    }

    @Test
    void oobFrameWriteFlushFailureCompletesOobPromiseExceptionally() {
        // Given: a router that responds with a RespondWith body
        var cause = new RuntimeException("write failed");
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise);
        oob.setRouteName(DEFAULT_ROUTE);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, new ProduceResponseData(), false)));
        var handler = handlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);
        channel.pipeline().addFirst("failWrites", new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise p) {
                ReferenceCountUtil.release(msg);
                p.setFailure(cause);
            }
        });

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then: OOB promise is completed exceptionally with the write failure cause
        assertThat(promise).isCompletedExceptionally();
        assertThatThrownBy(promise::join).hasCause(cause);
    }

    @Test
    void executeOnEventLoopSynchronousThrowCompletesExceptionally() {
        // Given: routes.get() throws synchronously; executor is a real single-threaded group
        // whose inEventLoop() returns false on the test thread, forcing the bridge path.
        var throwingRoutes = new HashMap<String, RouteDescriptor>() {
            @Override
            public RouteDescriptor get(Object key) {
                throw new IllegalStateException("sync throw from routes");
            }
        };
        var handler = new RouterDispatchHandler(router, throwingRoutes, Map.of(), ccsm, "test-cluster",
                new IdentityNodeIdMapping(DEFAULT_ROUTE), null);

        try (var executorGroup = new DefaultEventExecutorGroup(1)) {
            // Wire ctx so that ctx.executor() returns the real (off-test-thread) executor
            var mockCtx = mock(ChannelHandlerContext.class);
            when(mockCtx.executor()).thenReturn(executorGroup.next());
            handler.handlerAdded(mockCtx);

            // When: called from the test thread — inEventLoop() is false, bridge path taken
            var stage = handler.sendToAnyNode(DEFAULT_ROUTE, new RequestHeaderData(),
                    new ProduceRequestData(), "session", 1);

            // Then: bridge completes exceptionally within a bounded time (not hanging)
            assertThat(stage.toCompletableFuture()).failsWithin(Duration.ofSeconds(5));
        }
    }

    private InternalRequestFrame<ProduceRequestData> oobProduceFrame(int correlationId, CompletableFuture<?> promise) {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9)
                .setCorrelationId(correlationId);
        return new InternalRequestFrame<>((short) 9, correlationId, true, filter, promise, header, new ProduceRequestData());
    }
}
