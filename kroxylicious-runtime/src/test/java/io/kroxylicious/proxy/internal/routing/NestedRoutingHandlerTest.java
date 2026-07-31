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
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.CorrelationIdAllocator;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NestedRoutingHandlerTest {

    private static final String ACTIVATION_ROUTE = "route-to-nested";
    private static final String NESTED_ROUTER_NAME = "inner-router";
    private static final String VIRTUAL_CLUSTER = "test-cluster";
    private static final String SESSION_ID = "test-session";
    private static final int CORRELATION_ID = 42;

    @Mock
    private RouterChainFactory routerChainFactory;

    @Mock
    private Router router;

    @Mock
    private CorrelationIdAllocator correlationIdAllocator;

    private EmbeddedChannel channel;

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    private static RouteDescriptor clusterRoute(String name, int id) {
        return new RouteDescriptor(name, id, new TargetCluster("broker:9092", null), null, List.of());
    }

    private NestedRoutingHandler createHandler(Map<String, RouteDescriptor> nestedRoutes) {
        NodeIdMapping mapping = nestedRoutes.size() == 1
                ? new IdentityNodeIdMapping(nestedRoutes.keySet().iterator().next())
                : new BijectiveNodeIdMapping(
                        nestedRoutes.entrySet().stream()
                                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().id())),
                        nestedRoutes.size());
        return new NestedRoutingHandler(
                ACTIVATION_ROUTE,
                NESTED_ROUTER_NAME,
                VIRTUAL_CLUSTER,
                routerChainFactory,
                nestedRoutes,
                mapping,
                correlationIdAllocator,
                SESSION_ID,
                Subject.anonymous(),
                null);
    }

    private static DecodedRequestFrame<FetchRequestData> fetchFrame(int correlationId, String routeName) {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12)
                .setCorrelationId(correlationId);
        var frame = new DecodedRequestFrame<>((short) 12, correlationId, true, header, new FetchRequestData());
        frame.setRouteName(routeName);
        return frame;
    }

    private static DecodedRequestFrame<MetadataRequestData> metadataFrame(int correlationId, String routeName) {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.METADATA.id)
                .setRequestApiVersion((short) 12)
                .setCorrelationId(correlationId);
        var frame = new DecodedRequestFrame<>((short) 12, correlationId, true, header, new MetadataRequestData());
        frame.setRouteName(routeName);
        return frame;
    }

    private static DecodedRequestFrame<ProduceRequestData> produceFrame(int correlationId, String routeName) {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9)
                .setCorrelationId(correlationId);
        var frame = new DecodedRequestFrame<>((short) 9, correlationId, true, header, new ProduceRequestData());
        frame.setRouteName(routeName);
        return frame;
    }

    // --- channelRead: routing ---

    @Test
    void shouldPassThroughFrameWhenRouteDoesNotMatchActivation() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        var frame = fetchFrame(CORRELATION_ID, "other-route");

        // When
        channel.writeInbound(frame);

        // Then
        DecodedRequestFrame<?> passed = channel.readInbound();
        assertThat(passed).isSameAs(frame);
    }

    @Test
    void shouldPassThroughNonDecodedFrameMessage() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound("not-a-frame");

        // Then
        assertThat((Object) channel.readInbound()).isEqualTo("not-a-frame");
    }

    @Test
    void shouldDispatchToNestedRouterWhenRouteMatches() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));

        // When
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        verify(router).onRequest(any(), anyShort(), any(), any(), any());
    }

    @Test
    void shouldLazilyCreateNestedRouter() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));

        // When
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.writeInbound(fetchFrame(CORRELATION_ID + 1, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        verify(routerChainFactory, times(1)).createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER);
    }

    @Test
    void shouldQualifyStaticRouteNameWithRouterPrefix() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of(ApiKeys.FETCH, "inner-r"));

        // When
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));

        // Then
        DecodedRequestFrame<?> forwarded = channel.readInbound();
        assertThat(forwarded).isNotNull();
        assertThat(forwarded.routeName()).isEqualTo(NESTED_ROUTER_NAME + "/inner-r");
    }

    @Test
    void shouldTrackPendingStaticRouteForNodeIdTranslationApi() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of(ApiKeys.METADATA, "inner-r"));

        // When
        channel.writeInbound(metadataFrame(CORRELATION_ID, ACTIVATION_ROUTE));

        // Then
        DecodedRequestFrame<?> forwarded = channel.readInbound();
        assertThat(forwarded).isNotNull();
        assertThat(forwarded.routeName()).isEqualTo(NESTED_ROUTER_NAME + "/inner-r");
    }

    // --- dynamic dispatch outcomes ---

    @Test
    void shouldWriteResponseForRespondWith() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        var responseBody = new ProduceResponseData();
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, responseBody, false)));

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.body()).isSameAs(responseBody);
        assertThat(out.header().correlationId()).isEqualTo(CORRELATION_ID);
    }

    @Test
    void shouldWriteErrorResponseForRespondWithError() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
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
        channel.writeInbound(produceFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.header().correlationId()).isEqualTo(CORRELATION_ID);
    }

    @Test
    void shouldNotWriteResponseForRespondWithoutReply() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    void shouldWriteErrorResponseWhenFutureFails() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));

        // When
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.header().correlationId()).isEqualTo(CORRELATION_ID);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldWriteErrorResponseForUnrecognisedResponseType() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        RouterResponse unrecognised = new RouterResponse() {
        };
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn((CompletableFuture) CompletableFuture.completedFuture(unrecognised));

        // When
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.header().correlationId()).isEqualTo(CORRELATION_ID);
    }

    @Test
    void shouldNotCloseChannelForNestedCloseConnectionRequest() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        var responseBody = new ProduceResponseData();
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, responseBody, true)));

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        assertThat(channel.isOpen()).isTrue();
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
    }

    // --- write: response interception ---

    @Test
    void shouldInterceptAndCompletePendingNestedResponse() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        var future = new CompletableFuture<org.apache.kafka.common.protocol.ApiMessage>();
        var pending = new RouterDispatchHandler.PendingResponse(
                future, "inner-r", new IdentityNodeIdMapping("inner-r"));
        int routingCorrelationId = 100;
        handler.pendingResponses.put(routingCorrelationId, pending);

        var responseBody = new ProduceResponseData();
        var responseFrame = new DecodedResponseFrame<>((short) 9, routingCorrelationId,
                new ResponseHeaderData(), responseBody);

        // When
        channel.writeOutbound(responseFrame);

        // Then
        assertThat(future).isCompletedWithValue(responseBody);
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    void shouldTranslateNodeIdsForPendingStaticRoute() {
        // Given
        var nestedRoutes = Map.of(
                "r1", clusterRoute("r1", 0),
                "r2", clusterRoute("r2", 1));
        var handler = createHandler(nestedRoutes);
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of(ApiKeys.METADATA, "r1"));

        channel.writeInbound(metadataFrame(CORRELATION_ID, ACTIVATION_ROUTE));

        var md = new MetadataResponseData();
        md.setControllerId(0);
        md.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(0).setHost("h0").setPort(9092));
        var responseFrame = new DecodedResponseFrame<>((short) 12, CORRELATION_ID,
                new ResponseHeaderData(), md);

        // When
        channel.writeOutbound(responseFrame);

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        var translatedMd = (MetadataResponseData) out.body();
        assertThat(translatedMd.controllerId()).isZero();
    }

    @Test
    void shouldPassThroughUnmatchedResponse() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        var responseFrame = new DecodedResponseFrame<>((short) 9, 999,
                new ResponseHeaderData(), new ProduceResponseData());

        // When
        channel.writeOutbound(responseFrame);

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isSameAs(responseFrame);
    }

    @Test
    void shouldPassThroughNonDecodedResponseOnWrite() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeOutbound("not-a-frame");

        // Then
        assertThat((Object) channel.readOutbound()).isEqualTo("not-a-frame");
    }

    // --- handlerRemoved ---

    @Test
    void shouldCompletePendingFuturesExceptionallyOnRemoval() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        var future1 = new CompletableFuture<org.apache.kafka.common.protocol.ApiMessage>();
        var future2 = new CompletableFuture<org.apache.kafka.common.protocol.ApiMessage>();
        handler.pendingResponses.put(100, new RouterDispatchHandler.PendingResponse(
                future1, "inner-r", new IdentityNodeIdMapping("inner-r")));
        handler.pendingResponses.put(101, new RouterDispatchHandler.PendingResponse(
                future2, "inner-r", new IdentityNodeIdMapping("inner-r")));

        // When
        channel.pipeline().remove(handler);

        // Then
        assertThat(future1).isCompletedExceptionally();
        assertThat(future2).isCompletedExceptionally();
    }

    @Test
    void shouldCloseNestedRouterOnRemoval() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // When
        channel.pipeline().remove(handler);

        // Then
        verify(router).close();
    }

    @Test
    void shouldNotFailOnRemovalWithNoPendingState() {
        // Given
        var handler = createHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);

        // When / Then
        channel.pipeline().remove(handler);
    }
}
