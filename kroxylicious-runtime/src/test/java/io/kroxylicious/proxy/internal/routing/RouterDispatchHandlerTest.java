/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.router.Router;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
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

    private EmbeddedChannel channel;

    private RouterDispatchHandler handlerWithIdentityMapping(Map<ApiKeys, String> staticRoutes) {
        return new RouterDispatchHandler(
                router, Map.of(), staticRoutes, ccsm, new IdentityNodeIdMapping(DEFAULT_ROUTE));
    }

    @Test
    void shouldForwardNonFrameMessageToCcsm() {
        // Given
        var handler = handlerWithIdentityMapping(Map.of());
        channel = new EmbeddedChannel(handler);

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
        channel = new EmbeddedChannel(handler);

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
        channel = new EmbeddedChannel(handler);

        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true,
                new RequestHeaderData(), new FetchRequestData());

        // When
        channel.writeInbound(frame);

        // Then
        verify(ccsm).forwardToRoute(DEFAULT_ROUTE, frame);
    }

    @Test
    void shouldForwardStaticallyRoutedOpaqueFrameViaForwardToRoute() {
        // Given
        var staticRoutes = Map.of(ApiKeys.FETCH, DEFAULT_ROUTE);
        var handler = handlerWithIdentityMapping(staticRoutes);
        channel = new EmbeddedChannel(handler);

        var buf = Unpooled.buffer();
        var opaqueFrame = new OpaqueRequestFrame(buf, ApiKeys.FETCH.id, (short) 12, CORRELATION_ID, false, 0, true);

        // When
        channel.writeInbound(opaqueFrame);

        // Then
        verify(ccsm).forwardToRoute(DEFAULT_ROUTE, opaqueFrame);
        buf.release();
    }

    @Test
    void shouldTranslateNodeIdsInMetadataResponse() {
        // Given: bijective mapping with two routes; METADATA statically routed to route-a
        var mapping = new BijectiveNodeIdMapping(Map.of("route-a", 0, "route-b", 1), 2);
        var handler = new RouterDispatchHandler(
                router, Map.of(), Map.of(ApiKeys.METADATA, "route-a"), ccsm, mapping);
        channel = new EmbeddedChannel(handler);

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
                router, Map.of(routeName, rd), Map.of(), ccsm, new IdentityNodeIdMapping(routeName));
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
}
