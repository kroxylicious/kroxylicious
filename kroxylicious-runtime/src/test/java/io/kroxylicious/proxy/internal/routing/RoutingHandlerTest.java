/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.util.ReferenceCountUtil;

import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.ProduceResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.PathElement;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.CloseReason;
import io.kroxylicious.proxy.internal.CorrelationIdAllocator;
import io.kroxylicious.proxy.internal.InternalRequestFrame;
import io.kroxylicious.proxy.internal.InternalResponseFrame;
import io.kroxylicious.proxy.internal.topology.RequestSender;
import io.kroxylicious.proxy.internal.topology.TopologyCache;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterResponse;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("removal")
// Suppress removal warnings for deprecated Subject API
class RoutingHandlerTest {

    private static final int CORRELATION_ID = 42;
    // Without the event-loop hop roughly 0.1-0.3% of iterations drop a response, so this many fail almost every run.
    // The deterministic off-event-loop thread tests are the primary guard; this one shows the dropped-response symptom.
    private static final int RACE_ITERATIONS = 5_000;
    private static final String DEFAULT_ROUTE = "default";
    private static final String ACTIVATION_ROUTE = "route-to-nested";
    private static final PathElement.Route ACTIVATION_ROUTE_PATH = new PathElement.Route(ACTIVATION_ROUTE, PathElement.ClientOrigin.INSTANCE);
    private static final String NESTED_ROUTER_NAME = "inner-router";
    private static final String VIRTUAL_CLUSTER = "test-cluster";
    private static final String SESSION_ID = "test-session";

    @Mock
    private Router router;

    @Mock
    private ClientConnectionStateMachine ccsm;

    @Mock
    private RouterChainFactory routerChainFactory;

    @Mock
    private CorrelationIdAllocator correlationIdAllocator;

    private EmbeddedChannel channel;

    // Real event loop for tests that complete router futures off the loop; EmbeddedChannel's inEventLoop() is always true.
    // Created by registerOnEventLoop so tests that only use EmbeddedChannel don't allocate one.
    private DefaultEventLoop eventLoop;
    private LocalChannel localChannel;
    // Written only on eventLoop; read after a Future.get() barrier.
    private final List<Integer> writtenCorrelationIds = new ArrayList<>();

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
        if (localChannel != null) {
            localChannel.close().syncUninterruptibly();
        }
        if (eventLoop != null) {
            eventLoop.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    // ======================== Top-level helpers ========================

    private RoutingHandler topLevelHandler(Map<ApiKeys, String> staticRoutes) {
        var dispatcher = RouteDispatcher.forTopLevel(Map.of(), new IdentityNodeIdMapping(DEFAULT_ROUTE), new HashMap<>(), ccsm);
        return RoutingHandler.topLevel(dispatcher, router, staticRoutes, ccsm, null);
    }

    private RoutingHandler topLevelHandlerWithRoute(String routeName) {
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());
        return unstubbedTopLevelHandlerWithRoute(routeName);
    }

    private RoutingHandler unstubbedTopLevelHandlerWithRoute(String routeName) {
        var rd = new RouteDescriptor(routeName, 0, new TargetCluster("localhost:9092", Optional.empty()), null, List.of());
        var dispatcher = RouteDispatcher.forTopLevel(Map.of(routeName, rd), new IdentityNodeIdMapping(routeName), new HashMap<>(), ccsm);
        return RoutingHandler.topLevel(dispatcher, router, Map.of(), ccsm, null);
    }

    private EmbeddedChannel channelWithTerminal(RoutingHandler handler) {
        return new EmbeddedChannel(handler, new RoutingTerminalHandler(ccsm));
    }

    // ======================== Nested helpers ========================

    private static RouteDescriptor clusterRoute(String name, int id) {
        return new RouteDescriptor(name, id, new TargetCluster("broker:9092", Optional.empty()), null, List.of());
    }

    private RoutingHandler nestedHandler(Map<String, RouteDescriptor> nestedRoutes) {
        NodeIdMapping mapping = nestedRoutes.size() == 1
                ? new IdentityNodeIdMapping(nestedRoutes.keySet().iterator().next())
                : new BijectiveNodeIdMapping(
                        nestedRoutes.entrySet().stream()
                                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().id())),
                        nestedRoutes.size());
        return RoutingHandler.nested(ACTIVATION_ROUTE_PATH, NESTED_ROUTER_NAME, VIRTUAL_CLUSTER,
                routerChainFactory, nestedRoutes, mapping, correlationIdAllocator,
                new ConcurrentHashMap<>(), SESSION_ID, Subject::anonymous, null);
    }

    // ======================== Frame helpers ========================

    private static DecodedRequestFrame<ProduceRequestData> produceFrame(int correlationId) {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9)
                .setCorrelationId(correlationId);
        return new DecodedRequestFrame<>((short) 9, correlationId, true, header, new ProduceRequestData());
    }

    private static DecodedRequestFrame<FetchRequestData> fetchFrame(int correlationId, String routeName) {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12)
                .setCorrelationId(correlationId);
        var frame = new DecodedRequestFrame<>((short) 12, correlationId, true, header, new FetchRequestData());
        frame.setRouting(new PathElement.Route(routeName, PathElement.ClientOrigin.INSTANCE));
        return frame;
    }

    private static DecodedRequestFrame<ProduceRequestData> produceFrame(int correlationId, String routeName) {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9)
                .setCorrelationId(correlationId);
        var frame = new DecodedRequestFrame<>((short) 9, correlationId, true, header, new ProduceRequestData());
        frame.setRouting(new PathElement.Route(routeName, PathElement.ClientOrigin.INSTANCE));
        return frame;
    }

    private static OpaqueRequestFrame opaqueFrame(ApiKeys apiKey, int correlationId, String routeName) {
        var buf = Unpooled.buffer();
        var frame = new OpaqueRequestFrame(buf, apiKey.id, (short) 12, correlationId, false, 0, true);
        frame.setRouting(new PathElement.Route(routeName, PathElement.ClientOrigin.INSTANCE));
        return frame;
    }

    private InternalRequestFrame<ProduceRequestData> oobProduceFrame(int correlationId, CompletableFuture<?> promise) {
        return oobProduceFrame(correlationId, promise, null);
    }

    // A filter-issued out-of-band request (FilterContext.sendRequest) - top-level OOB completion
    // (handleOobCompletion) is reached only via a FilterOriginator, since a router's own
    // sendInternal fires downstream and never re-enters its own handler's channelRead.
    private InternalRequestFrame<ProduceRequestData> oobProduceFrame(int correlationId, CompletableFuture<?> promise, @Nullable String route) {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9)
                .setCorrelationId(correlationId);
        var frame = new InternalRequestFrame<>((short) 9, correlationId, true, header, new ProduceRequestData());
        frame.setRouting(new PathElement.FilterOriginator("test-filter", 0, promise,
                route == null ? PathElement.ClientOrigin.INSTANCE : new PathElement.Route(route, PathElement.ClientOrigin.INSTANCE)));
        return frame;
    }

    // A router-issued out-of-band request (RouterContext.sendRequest) targeting a route that
    // resolves to a nested router - nested OOB completion (handleNestedOobCompletion) is reached
    // only via a RouterOriginator whose underlying route matches the nested handler's own
    // activation path (see RouterRequestSource.intercepts).
    private InternalRequestFrame<ProduceRequestData> nestedOobProduceFrame(int correlationId, CompletableFuture<?> promise, String route) {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9)
                .setCorrelationId(correlationId);
        var frame = new InternalRequestFrame<>((short) 9, correlationId, true, header, new ProduceRequestData());
        frame.setRouting(new PathElement.RouterOriginator(promise, new PathElement.Route(route, PathElement.ClientOrigin.INSTANCE)));
        return frame;
    }

    // ========================================================================
    // Top-level tests
    // ========================================================================

    @Test
    void topLevel_shouldForwardNonFrameMessageDownstream() {
        // Given
        var handler = topLevelHandler(Map.of());
        channel = channelWithTerminal(handler);

        // When
        channel.writeInbound("not-a-frame");

        // Then
        verify(ccsm).onClientFilterChainComplete("not-a-frame");
    }

    @Test
    void topLevel_shouldDispatchDynamicallyForDecodedFrame() {
        // Given
        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true,
                new RequestHeaderData(), new FetchRequestData());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = topLevelHandler(Map.of());
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(frame);

        // Then
        verify(router).onRequest(any(ApiKeys.class), anyShort(), any(), any(), any());
    }

    @Test
    void topLevel_shouldForwardOpaqueFrameNotInStaticRoutes() {
        // Given
        var staticRoutes = Map.of(ApiKeys.PRODUCE, DEFAULT_ROUTE);
        var handler = topLevelHandler(staticRoutes);
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
    void topLevel_shouldForwardStaticallyRoutedFrame() {
        // Given
        var staticRoutes = Map.of(ApiKeys.FETCH, DEFAULT_ROUTE);
        var handler = topLevelHandler(staticRoutes);
        channel = channelWithTerminal(handler);

        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true,
                new RequestHeaderData(), new FetchRequestData());

        // When
        channel.writeInbound(frame);

        // Then
        verify(ccsm).forwardToRoute(DEFAULT_ROUTE, frame);
        assertThat(frame.routing()).isEqualTo(new PathElement.Route(DEFAULT_ROUTE, PathElement.ClientOrigin.INSTANCE));
    }

    @Test
    void topLevel_oobFrameForStaticallyRoutedApiKeyShouldBeForwardedWithoutInvokingRouter() {
        // Given: PRODUCE is declared static, so the router's onRequest must never be consulted
        var staticRoutes = Map.of(ApiKeys.PRODUCE, DEFAULT_ROUTE);
        var handler = topLevelHandler(staticRoutes);
        channel = new EmbeddedChannel(handler);
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise);

        // When
        channel.writeInbound(oob);

        // Then: the resolved route is grafted onto the OOB frame's own FilterOriginator (not used
        // to replace it) so the response can still be recognised as this filter's own
        InternalRequestFrame<?> forwarded = channel.readInbound();
        assertThat(forwarded).isNotNull();
        assertThat(forwarded.routing())
                .isEqualTo(new PathElement.FilterOriginator("test-filter", 0, promise, new PathElement.Route(DEFAULT_ROUTE, PathElement.ClientOrigin.INSTANCE)));
        verify(router, never()).onRequest(any(), anyShort(), any(), any(), any());
    }

    @Test
    void topLevel_oobFrameForNodeIdTranslationApiShouldNotTrackStaticRoute() {
        // Given: every OOB frame shares the same reserved correlation ID, so tracking it for
        // node-ID translation would collide with any other concurrently in-flight, statically-routed
        // OOB request for a NODE_ID_TRANSLATION_APIS key.
        var staticRoutes = Map.of(ApiKeys.PRODUCE, DEFAULT_ROUTE);
        var handler = topLevelHandler(staticRoutes);
        channel = new EmbeddedChannel(handler);
        var oob = oobProduceFrame(CORRELATION_ID, new CompletableFuture<>());

        // When
        channel.writeInbound(oob);

        // Then
        assertThat(handler.dispatcher().hasPendingStaticRoute(oob.correlationId())).isFalse();
    }

    @Test
    void topLevel_shouldTranslateNodeIdsInMetadataResponse() {
        // Given
        var mapping = new BijectiveNodeIdMapping(Map.of("route-a", 0, "route-b", 1), 2);
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        var dispatcher = RouteDispatcher.forTopLevel(Map.of(), mapping, new HashMap<>(), ccsm);
        var handler = RoutingHandler.topLevel(dispatcher, router, Map.of(ApiKeys.METADATA, "route-a"), ccsm, null);
        channel = channelWithTerminal(handler);

        var requestFrame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true,
                new RequestHeaderData(), new MetadataRequestData());
        channel.writeInbound(requestFrame);

        // When
        var md = new MetadataResponseData();
        md.setControllerId(0);
        md.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(0).setHost("h0").setPort(9092));
        md.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(1).setHost("h1").setPort(9093));
        var responseFrame = new DecodedResponseFrame<>((short) 12, CORRELATION_ID, new ResponseHeaderData(), md);
        channel.writeOutbound(responseFrame);

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        var translatedMd = (MetadataResponseData) out.body();
        assertThat(translatedMd.brokers().find(0)).isNotNull();
        assertThat(translatedMd.brokers().find(2)).isNotNull();
        assertThat(translatedMd.controllerId()).isZero();
    }

    @Test
    void topLevel_shouldSetClientCorrelationIdOnRespondWithResponse() {
        // Given
        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);
        var body = new MetadataRequestData();
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, body, false)));

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.header().correlationId())
                .as("header.correlationId() must match the client's correlation ID so the wire format is correct")
                .isEqualTo(CORRELATION_ID);
    }

    @Test
    void topLevel_shouldSetClientCorrelationIdOnRespondWithErrorResponse() {
        // Given
        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);
        var requestHeader = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9)
                .setCorrelationId(CORRELATION_ID);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithError(
                                requestHeader, new ProduceRequestData().setAcks((short) 1),
                                Errors.UNKNOWN_SERVER_ERROR, "test", false)));

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.header().correlationId()).isEqualTo(CORRELATION_ID);
    }

    @Test
    void topLevel_shouldCloseChannelWhenRouterFutureFails() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));
        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void topLevel_shouldCloseChannelWhenRouterThrowsSynchronously() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenThrow(new RuntimeException("boom"));
        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void topLevel_shouldCloseChannelWhenRouterReturnsNull() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void topLevel_shouldCloseChannelWhenRouterIssuedResponseIsUnclaimed() {
        // Given: a RouterOriginator whose route lineage doesn't match this (top-level, parentPath =
        // null) dispatcher - as if a router-issued out-of-band request's response arrived without
        // any level having claimed it
        var handler = topLevelHandler(Map.of());
        channel = new EmbeddedChannel(handler);
        var strayRouting = new PathElement.RouterOriginator(new CompletableFuture<>(),
                new PathElement.Route("route-a", new PathElement.Route("outer", PathElement.ClientOrigin.INSTANCE)));
        var responseFrame = new DecodedResponseFrame<>((short) 12, CORRELATION_ID, new ResponseHeaderData(), new MetadataResponseData());
        responseFrame.setRouting(strayRouting);

        // When
        channel.writeOutbound(responseFrame);

        // Then
        assertThat(channel.isOpen()).isFalse();
        assertThat((DecodedResponseFrame<?>) channel.readOutbound()).isNull();
    }

    @Test
    void topLevel_shouldCloseChannelWhenRouterIssuedResponseInvariantViolated() {
        // Given: a RouterOriginator whose route matches this (top-level) dispatcher's own
        // level structurally, but whose future was never registered as pending here - an
        // internal bookkeeping invariant violation, not a legitimately unclaimed response.
        var handler = topLevelHandler(Map.of());
        channel = new EmbeddedChannel(handler);
        var routing = new PathElement.RouterOriginator(new CompletableFuture<>(),
                new PathElement.Route(DEFAULT_ROUTE, PathElement.ClientOrigin.INSTANCE));
        var responseFrame = new DecodedResponseFrame<>((short) 12, CORRELATION_ID, new ResponseHeaderData(), new MetadataResponseData());
        responseFrame.setRouting(routing);

        // When
        channel.writeOutbound(responseFrame);

        // Then
        assertThat(channel.isOpen()).isFalse();
        assertThat((DecodedResponseFrame<?>) channel.readOutbound()).isNull();
    }

    @Test
    void topLevel_shouldDeliverResponseBeforeClosingChannelWhenRespondWithAndCloseConnection() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, new MetadataRequestData(), true)));
        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out)
                .as("response must be delivered even when the router also requests connection close")
                .isNotNull();
        assertThat(out.header().correlationId()).isEqualTo(CORRELATION_ID);
        verify(ccsm).requestClose(CloseReason.routerRequested());
    }

    @Test
    void topLevel_shouldDeliverResponseBeforeClosingChannelWhenRespondWithAndCloseConnectionAndResponseIsOutOfSequence() {
        // Given
        var pendingFuture = new CompletableFuture<RouterResponse>();
        var responseBody = new ProduceResponseData();
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(pendingFuture.minimalCompletionStage())
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, responseBody, true)));
        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When: two requests in flight; request 2 resolves immediately with close, but request 1
        // is still pending so request 2's response is buffered in the sequencer at slot 1.
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.writeInbound(produceFrame(CORRELATION_ID + 1));
        channel.runPendingTasks();
        // Completing request 1 with no-reply advances the sequencer, which drains the buffered
        // close-response at slot 1 and writes it to the channel.
        pendingFuture.complete(new RouterResponseImpl.RespondWithoutReply(false));
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out)
                .as("response for the close-triggering request must be delivered even when it arrives out of sequence")
                .isNotNull();
        assertThat(out.header().correlationId()).isEqualTo(CORRELATION_ID + 1);
        verify(ccsm).requestClose(CloseReason.routerRequested());
    }

    @Test
    void topLevel_shouldHandleRouterCompletionOnEventLoopWhenRouterCompletesOffEventLoop() throws Exception {
        // Given
        var completionThread = captureRoutedRequestCompleteThread();
        var pendingFuture = routeOnEventLoop(topLevelHandlerWithRoute(DEFAULT_ROUTE), produceFrame(CORRELATION_ID));

        // When
        completeOffEventLoop(() -> pendingFuture.complete(produceResponse()));

        // Then
        assertHandledOnEventLoop(completionThread.get());
        assertThat(writtenCorrelationIds).containsExactly(CORRELATION_ID);
    }

    @Test
    void topLevel_shouldHandleRouterFailureOnEventLoopWhenRouterFailsOffEventLoop() throws Exception {
        // Given
        var completionThread = captureRoutedRequestCompleteThread();
        var pendingFuture = routeOnEventLoop(topLevelHandlerWithRoute(DEFAULT_ROUTE), produceFrame(CORRELATION_ID));

        // When
        completeOffEventLoop(() -> pendingFuture.completeExceptionally(new IllegalStateException("router failed")));

        // Then
        assertHandledOnEventLoop(completionThread.get());
        assertThat(localChannel.closeFuture()).succeedsWithin(5, TimeUnit.SECONDS);
    }

    @Test
    void topLevel_shouldHandleRouterCompletionInlineWhenRouterCompletesOnEventLoop() throws Exception {
        // Given
        var completionThread = captureRoutedRequestCompleteThread();
        var pendingFuture = routeOnEventLoop(topLevelHandlerWithRoute(DEFAULT_ROUTE), produceFrame(CORRELATION_ID));

        // When
        var handledBeforeCompleteReturned = eventLoop.submit(() -> {
            pendingFuture.complete(produceResponse());
            return completionThread.get() != null;
        }).get(5, TimeUnit.SECONDS);

        // Then
        assertThat(handledBeforeCompleteReturned)
                .as("a completion already on the event loop must be handled inline, not re-queued")
                .isTrue();
    }

    @Test
    void topLevel_shouldHandleOobRouterCompletionOnEventLoopWhenRouterCompletesOffEventLoop() throws Exception {
        // Given
        var completionThread = captureRoutedRequestCompleteThread();
        var promise = new CompletableFuture<ProduceResponseData>();
        var routerFuture = routeOnEventLoop(topLevelHandlerWithRoute(DEFAULT_ROUTE), oobProduceFrame(CORRELATION_ID, promise, DEFAULT_ROUTE));

        // When
        completeOffEventLoop(() -> routerFuture.complete(oobErrorResponse()));

        // Then
        assertHandledOnEventLoop(completionThread.get());
        assertThat(promise).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(ExecutionException.class)
                .withMessageContaining("oob-error");
    }

    @Test
    void nested_shouldHandleOobRouterCompletionOnEventLoopWhenRouterCompletesOffEventLoop() throws Exception {
        // Given
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        var promise = new CompletableFuture<ProduceResponseData>();
        // The nested handler has no ccsm; a dependent registered now runs on whichever thread fails the promise.
        var completionThread = promise.handle((response, error) -> Thread.currentThread());
        var routerFuture = routeOnEventLoop(nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0))),
                nestedOobProduceFrame(CORRELATION_ID, promise, ACTIVATION_ROUTE));

        // When
        completeOffEventLoop(() -> routerFuture.complete(oobErrorResponse()));

        // Then
        assertThat(completionThread).succeedsWithin(Duration.ofSeconds(5))
                .satisfies(this::assertHandledOnEventLoop);
        assertThat(promise).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(ExecutionException.class)
                .withMessageContaining("oob-error");
    }

    @Test
    void topLevel_shouldNotDropBufferedResponseWhenEarlierRequestCompletesOffEventLoopConcurrently() throws Exception {
        // Given
        var routerFutures = new ConcurrentLinkedQueue<CompletableFuture<RouterResponse>>();
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenAnswer(invocation -> routerFutures.remove().minimalCompletionStage());
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());
        int dropped = 0;

        // When
        try (var offLoopThread = Executors.newSingleThreadExecutor()) {
            for (int i = 0; i < RACE_ITERATIONS; i++) {
                if (!raceOutOfOrderCompletion(routerFutures, offLoopThread)) {
                    dropped++;
                }
            }
        }

        // Then
        assertThat(dropped)
                .as("iterations (of %d) in which the buffered response for request 2 was dropped", RACE_ITERATIONS)
                .isZero();
    }

    /**
     * Completes request 2 on the event loop and request 1 off it simultaneously; returns whether request 2's response was written.
     */
    private boolean raceOutOfOrderCompletion(Queue<CompletableFuture<RouterResponse>> routerFutures, ExecutorService offLoopThread) throws Exception {
        var first = new CompletableFuture<RouterResponse>();
        var second = new CompletableFuture<RouterResponse>();
        routerFutures.add(first);
        routerFutures.add(second);
        writtenCorrelationIds.clear();
        registerOnEventLoop(unstubbedTopLevelHandlerWithRoute(DEFAULT_ROUTE));
        readOnEventLoop(produceFrame(CORRELATION_ID));
        readOnEventLoop(produceFrame(CORRELATION_ID + 1));

        var start = new CyclicBarrier(2);
        // Timed awaits so a failed iteration cannot strand the event loop or executor at the barrier.
        var onLoop = eventLoop.submit(() -> {
            start.await(5, TimeUnit.SECONDS);
            return second.complete(produceResponse());
        });
        var offLoop = offLoopThread.submit(() -> {
            start.await(5, TimeUnit.SECONDS);
            return first.complete(new RouterResponseImpl.RespondWithoutReply(false));
        });
        onLoop.get(5, TimeUnit.SECONDS);
        offLoop.get(5, TimeUnit.SECONDS);
        // Barrier: any completion hopped onto the event loop has run once this task does.
        boolean delivered = eventLoop.submit(() -> writtenCorrelationIds.contains(CORRELATION_ID + 1)).get(5, TimeUnit.SECONDS);
        localChannel.close().get(5, TimeUnit.SECONDS);
        return delivered;
    }

    private AtomicReference<Thread> captureRoutedRequestCompleteThread() {
        var thread = new AtomicReference<Thread>();
        doAnswer(invocation -> {
            thread.set(Thread.currentThread());
            return null;
        }).when(ccsm).onRoutedRequestComplete();
        return thread;
    }

    private void assertHandledOnEventLoop(Thread completionThread) {
        assertThat(eventLoop.inEventLoop(completionThread))
                .as("router completion must be handled on the channel's event loop, not the thread that completed the router future")
                .isTrue();
    }

    private CompletableFuture<RouterResponse> routeOnEventLoop(RoutingHandler handler, Object frame) throws Exception {
        var routerFuture = new CompletableFuture<RouterResponse>();
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(routerFuture.minimalCompletionStage());
        registerOnEventLoop(handler);
        readOnEventLoop(frame);
        return routerFuture;
    }

    private static RouterResponse oobErrorResponse() {
        return new RouterResponseImpl.RespondWithError(new RequestHeaderData(), new ProduceRequestData(), Errors.UNKNOWN_SERVER_ERROR, "oob-error", false);
    }

    private static RouterResponse produceResponse() {
        return new RouterResponseImpl.RespondWith(null, new ProduceResponseData(), false);
    }

    private void registerOnEventLoop(RoutingHandler handler) throws Exception {
        if (eventLoop == null) {
            eventLoop = new DefaultEventLoop();
        }
        localChannel = new LocalChannel();
        eventLoop.register(localChannel).get(5, TimeUnit.SECONDS);
        localChannel.pipeline().addLast(handler, new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise p) {
                if (msg instanceof DecodedResponseFrame<?> frame) {
                    writtenCorrelationIds.add(frame.header().correlationId());
                }
                ReferenceCountUtil.release(msg);
                p.setSuccess();
            }
        });
    }

    private void readOnEventLoop(Object msg) throws Exception {
        eventLoop.submit(() -> localChannel.pipeline().fireChannelRead(msg)).get(5, TimeUnit.SECONDS);
    }

    private void completeOffEventLoop(Runnable completer) throws Exception {
        CompletableFuture.runAsync(completer).get(5, TimeUnit.SECONDS);
        // Barrier: the completion hop is queued on the event loop before runAsync returns, so this task runs after it.
        eventLoop.submit(() -> null).get(5, TimeUnit.SECONDS);
    }

    @Test
    void topLevel_shouldCallOnRoutedRequestCompleteAfterDynamicDispatch() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));
        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        verify(ccsm).onRoutedRequestComplete();
    }

    @Test
    void topLevel_shouldNotWriteOutboundFrameForRespondWithoutReply() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));
        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.runPendingTasks();

        // Then
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    void topLevel_respondWithoutReplyShouldNotBlockSubsequentResponse() {
        // Given
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new RouterResponseImpl.RespondWithoutReply(false)))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, new MetadataResponseData(), false)));
        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID));
        channel.writeInbound(produceFrame(CORRELATION_ID + 1));
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.correlationId()).isEqualTo(CORRELATION_ID + 1);
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    void topLevel_shouldCloseRouterWhenHandlerRemoved() {
        // Given
        var handler = topLevelHandler(Map.of());
        channel = new EmbeddedChannel(handler);

        // When
        channel.pipeline().remove(handler);

        // Then
        verify(router).close();
    }

    @Test
    void topLevel_writeShouldPassThroughNonFrame() {
        // Given
        var handler = topLevelHandler(Map.of());
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeOutbound("not-a-frame");

        // Then
        assertThat((Object) channel.readOutbound()).isEqualTo("not-a-frame");
    }

    @Test
    void topLevel_writeShouldPassThroughFrameWithNonRoutingCorrelationId() {
        // Given
        var handler = topLevelHandler(Map.of());
        channel = new EmbeddedChannel(handler);
        var frame = new DecodedResponseFrame<>((short) 9, 99, new ResponseHeaderData(), new ProduceResponseData());

        // When
        channel.writeOutbound(frame);

        // Then
        assertThat((Object) channel.readOutbound()).isSameAs(frame);
    }

    @Test
    void topLevel_pendingFuturesCompletedExceptionallyWhenConnectionCloses() {
        // Given
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        when(ccsm.internalCorrelationIdAllocator()).thenReturn(new CorrelationIdAllocator(Integer.MIN_VALUE, 0));
        var rd = new RouteDescriptor(DEFAULT_ROUTE, 0, new TargetCluster("localhost:9092", Optional.empty()), null, List.of());
        var dispatcher = RouteDispatcher.forTopLevel(Map.of(DEFAULT_ROUTE, rd), new IdentityNodeIdMapping(DEFAULT_ROUTE), new HashMap<>(), ccsm);
        var handler = RoutingHandler.topLevel(dispatcher, router, Map.of(), ccsm, null);
        channel = channelWithTerminal(handler);
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12);
        var future = handler.dispatcher().sendToAnyNode(DEFAULT_ROUTE, header, new FetchRequestData(), SESSION_ID, 100)
                .toCompletableFuture();
        assertThat(future).isNotDone();

        // When
        channel.close().syncUninterruptibly();

        // Then
        assertThat(future).isCompletedExceptionally();
    }

    // ========================================================================
    // Top-level OOB tests
    // ========================================================================

    @Test
    void topLevel_oobFrameRespondWithShouldDeliverInternalResponseFrame() {
        // Given
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise, DEFAULT_ROUTE);
        var body = new ProduceResponseData();
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new RouterResponseImpl.RespondWith(null, body, false)));
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then
        var out = channel.readOutbound();
        assertThat(out).isInstanceOf(InternalResponseFrame.class);
        InternalResponseFrame<?> irf = (InternalResponseFrame<?>) out;
        assertThat(irf.correlationId()).isEqualTo(CORRELATION_ID);
        assertThat(irf.routing()).isEqualTo(oob.routing());
        assertThat(irf.promise()).isSameAs(promise);
    }

    @Test
    void topLevel_oobFrameRespondWithErrorShouldCompletePromiseExceptionally() {
        // Given
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise, DEFAULT_ROUTE);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithError(new RequestHeaderData(), new ProduceRequestData(), Errors.UNKNOWN_SERVER_ERROR, "oob-error", false)));
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then
        assertThat(promise).isCompletedExceptionally();
    }

    @Test
    void topLevel_oobFrameRespondWithoutReplyShouldCompletePromise() {
        // Given
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise, DEFAULT_ROUTE);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new RouterResponseImpl.RespondWithoutReply(false)));
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then
        assertThat(promise).isDone();
    }

    @Test
    void topLevel_oobFrameWriteFlushFailureCompletesOobPromiseExceptionally() {
        // Given: a router that responds with a RespondWith body
        var cause = new RuntimeException("write failed");
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise, DEFAULT_ROUTE);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, new ProduceResponseData(), false)));
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
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
    void topLevel_oobFrameShouldSkipSequenceImmediately() {
        // Given
        var pendingOobFuture = new CompletableFuture<RouterResponse>();
        var oob = oobProduceFrame(CORRELATION_ID, new CompletableFuture<>());
        var regularFrame = produceFrame(CORRELATION_ID + 1);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(pendingOobFuture.minimalCompletionStage())
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWith(null, new ProduceResponseData(), false)));
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.writeInbound(regularFrame);
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out)
                .as("regular frame response must not be blocked by the pending OOB sequence slot")
                .isNotNull();
        assertThat(out.correlationId()).isEqualTo(CORRELATION_ID + 1);
    }

    @Test
    void topLevel_oobFrameErrorShouldCloseChannel() {
        // Given
        var oob = oobProduceFrame(CORRELATION_ID, new CompletableFuture<>());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void topLevel_oobFrameSynchronousThrowShouldCloseChannel() {
        // Given
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = oobProduceFrame(CORRELATION_ID, promise);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenThrow(new RuntimeException("boom"));
        when(ccsm.sessionId()).thenReturn(SESSION_ID);
        when(ccsm.authenticatedSubject()).thenReturn(Subject.anonymous());

        var handler = topLevelHandlerWithRoute(DEFAULT_ROUTE);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then: the connection closes, and the filter's promise fails with the exact exception thrown
        assertThat(channel.isOpen()).isFalse();
        assertThat(promise).failsWithin(Duration.ofSeconds(1))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(RuntimeException.class)
                .withMessageContaining("boom");
    }

    // ========================================================================
    // Nested tests
    // ========================================================================

    @Test
    void nested_shouldPassThroughFrameWhenRouteDoesNotMatchActivation() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        var frame = fetchFrame(CORRELATION_ID, "other-route");

        // When
        channel.writeInbound(frame);

        // Then
        DecodedRequestFrame<?> passed = channel.readInbound();
        assertThat(passed).isSameAs(frame);
    }

    @Test
    void nested_shouldPassThroughNonDecodedFrameMessage() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound("not-a-frame");

        // Then
        assertThat((Object) channel.readInbound()).isEqualTo("not-a-frame");
    }

    @Test
    void nested_shouldForwardOpaqueFrameViaNestedStaticRoute() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of(ApiKeys.FETCH, "inner-r"));
        var frame = opaqueFrame(ApiKeys.FETCH, CORRELATION_ID, ACTIVATION_ROUTE);

        // When
        channel.writeInbound(frame);

        // Then
        OpaqueRequestFrame forwarded = channel.readInbound();
        assertThat(forwarded).isSameAs(frame);
        assertThat(forwarded.routing()).isEqualTo(new PathElement.Route(NESTED_ROUTER_NAME + "/inner-r", ACTIVATION_ROUTE_PATH));
        frame.releaseBuffer();
    }

    @Test
    void nested_shouldCloseChannelWhenOpaqueFrameArrivesForDynamicRoute() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        var frame = opaqueFrame(ApiKeys.FETCH, CORRELATION_ID, ACTIVATION_ROUTE);

        // When
        channel.writeInbound(frame);

        // Then
        assertThat(channel.isOpen()).isFalse();
        frame.releaseBuffer();
    }

    @Test
    void nested_shouldDispatchToNestedRouterWhenRouteMatches() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
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
    void nested_shouldLazilyCreateNestedRouter() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));

        // When
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.writeInbound(fetchFrame(CORRELATION_ID + 1, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        verify(routerChainFactory, times(1)).createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class));
    }

    @Test
    void nested_shouldActivateTopologyCacheWhenOneExistsForRouter() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        var cache = new TopologyCache();
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(routerChainFactory.existingTopologyCache(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(Optional.of(cache));
        when(router.staticRoutes()).thenReturn(Map.of());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));

        // When
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        assertThat(handler.dispatcher().activeCache()).isSameAs(cache);
    }

    @Test
    void nested_shouldLeaveTopologyCacheUnactivatedWhenNoneExistsForRouter() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(routerChainFactory.existingTopologyCache(NESTED_ROUTER_NAME, VIRTUAL_CLUSTER)).thenReturn(Optional.empty());
        when(router.staticRoutes()).thenReturn(Map.of());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));

        // When
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        assertThat(handler.dispatcher().activeCache()).isNull();
    }

    @Test
    void nested_shouldQualifyStaticRouteNameWithRouterPrefix() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of(ApiKeys.FETCH, "inner-r"));

        // When
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));

        // Then
        DecodedRequestFrame<?> forwarded = channel.readInbound();
        assertThat(forwarded).isNotNull();
        assertThat(forwarded.routing()).isEqualTo(new PathElement.Route(NESTED_ROUTER_NAME + "/inner-r", ACTIVATION_ROUTE_PATH));
    }

    @Test
    void nested_shouldWriteResponseForRespondWith() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
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
    void nested_shouldWriteErrorResponseForRespondWithError() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        var requestHeader = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9)
                .setCorrelationId(CORRELATION_ID);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithError(
                                requestHeader, new ProduceRequestData().setAcks((short) 1),
                                Errors.UNKNOWN_SERVER_ERROR, "test", false)));

        // When
        channel.writeInbound(produceFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.header().correlationId()).isEqualTo(CORRELATION_ID);
    }

    @Test
    void nested_shouldNotWriteResponseForRespondWithoutReply() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
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
    void nested_shouldWriteErrorResponseWhenFutureFails() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
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

    @Test
    void nested_shouldNotCloseChannelForCloseConnectionRequest() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
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

    @Test
    void nested_shouldNotCloseChannelWhenRouterIssuedResponseInvariantViolated() {
        // Given: a RouterOriginator whose route matches this nested dispatcher's own level
        // structurally, but whose future was never registered as pending here. Nested routers
        // don't own the client connection, so unlike the top-level case this must not attempt
        // to close it.
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        var routing = new PathElement.RouterOriginator(new CompletableFuture<>(),
                new PathElement.Route("some-route", ACTIVATION_ROUTE_PATH));
        var responseFrame = new DecodedResponseFrame<>((short) 12, CORRELATION_ID, new ResponseHeaderData(), new MetadataResponseData());
        responseFrame.setRouting(routing);

        // When
        channel.writeOutbound(responseFrame);

        // Then
        assertThat(channel.isOpen()).isTrue();
        assertThat((DecodedResponseFrame<?>) channel.readOutbound()).isNull();
    }

    @Test
    void nested_shouldPassThroughUnmatchedResponse() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
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
    void nested_shouldPassThroughNonDecodedResponseOnWrite() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeOutbound("not-a-frame");

        // Then
        assertThat((Object) channel.readOutbound()).isEqualTo("not-a-frame");
    }

    @Test
    void nested_shouldCloseNestedRouterOnRemoval() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
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
    void nested_shouldNotFailOnRemovalWithNoPendingState() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);

        // When
        channel.pipeline().remove(handler);

        // Then
        assertThat(channel.pipeline().get(RoutingHandler.class)).isNull();
    }

    @Test
    void nested_shouldPropagateTargetVirtualNodeIdToRouterContext() {
        // Given
        var handler = nestedHandler(Map.of("r-a", clusterRoute("r-a", 0), "r-b", clusterRoute("r-b", 1)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        var ctxCaptor = org.mockito.ArgumentCaptor.forClass(io.kroxylicious.proxy.router.RouterContext.class);
        when(router.onRequest(any(), anyShort(), any(), any(), ctxCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));

        var frame = fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE);
        frame.setTargetVirtualNodeId(1);

        // When
        channel.writeInbound(frame);
        channel.runPendingTasks();

        // Then
        var ctx = ctxCaptor.getValue();
        assertThat(ctx.virtualNode()).isPresent();
        assertThat(ctx.virtualNode().get()).isInstanceOfSatisfying(VirtualNodeImpl.class,
                vn -> assertThat(vn.virtualNodeId()).isEqualTo(1));
    }

    @Test
    void nested_shouldFallBackToEndpointNodeIdWhenFrameHasNoTargetVirtualNodeId() {
        // Given
        var nestedRoutes = Map.of("inner-r", clusterRoute("inner-r", 0));
        NodeIdMapping mapping = new IdentityNodeIdMapping(nestedRoutes.keySet().iterator().next());
        var handler = RoutingHandler.nested(ACTIVATION_ROUTE_PATH, NESTED_ROUTER_NAME, VIRTUAL_CLUSTER,
                routerChainFactory, nestedRoutes, mapping, correlationIdAllocator,
                new ConcurrentHashMap<>(), SESSION_ID, Subject::anonymous, 42);
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        var ctxCaptor = org.mockito.ArgumentCaptor.forClass(io.kroxylicious.proxy.router.RouterContext.class);
        when(router.onRequest(any(), anyShort(), any(), any(), ctxCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(
                        new RouterResponseImpl.RespondWithoutReply(false)));

        // When: frame has no targetVirtualNodeId set
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then: should fall back to the endpoint nodeId (42)
        var ctx = ctxCaptor.getValue();
        assertThat(ctx.virtualNode()).isPresent();
        assertThat(ctx.virtualNode().get()).isInstanceOfSatisfying(VirtualNodeImpl.class,
                vn -> assertThat(vn.virtualNodeId()).isEqualTo(42));
    }

    // ========================================================================
    // Nested OOB tests
    // ========================================================================

    @Test
    void nested_oobFrameRespondWithShouldDeliverInternalResponseFrame() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = nestedOobProduceFrame(CORRELATION_ID, promise, ACTIVATION_ROUTE);
        var body = new ProduceResponseData();
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new RouterResponseImpl.RespondWith(null, body, false)));

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then
        var out = channel.readOutbound();
        assertThat(out).isInstanceOf(InternalResponseFrame.class);
        InternalResponseFrame<?> irf = (InternalResponseFrame<?>) out;
        assertThat(irf.correlationId()).isEqualTo(CORRELATION_ID);
        assertThat(irf.routing()).isEqualTo(oob.routing());
        assertThat(irf.promise()).isSameAs(promise);
    }

    @Test
    void nested_oobFrameSynchronousThrowShouldCompletePromiseExceptionally() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        var promise = new CompletableFuture<ProduceResponseData>();
        var oob = nestedOobProduceFrame(CORRELATION_ID, promise, ACTIVATION_ROUTE);
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenThrow(new RuntimeException("boom"));

        // When
        channel.writeInbound(oob);
        channel.runPendingTasks();

        // Then: nested handlers don't own the client connection, so only the promise is affected —
        // and it must fail with the exact exception thrown, not some substitute
        assertThat(promise).failsWithin(Duration.ofSeconds(1))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(RuntimeException.class)
                .withMessageContaining("boom");
    }

    @Test
    void nested_shouldWriteErrorResponseWhenRouterThrowsSynchronously() {
        // Given
        var handler = nestedHandler(Map.of("inner-r", clusterRoute("inner-r", 0)));
        channel = new EmbeddedChannel(handler);
        when(routerChainFactory.createRouter(eq(NESTED_ROUTER_NAME), eq(VIRTUAL_CLUSTER), any(RequestSender.class))).thenReturn(router);
        when(router.staticRoutes()).thenReturn(Map.of());
        when(router.onRequest(any(), anyShort(), any(), any(), any()))
                .thenThrow(new RuntimeException("boom"));

        // When
        channel.writeInbound(fetchFrame(CORRELATION_ID, ACTIVATION_ROUTE));
        channel.runPendingTasks();

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.header().correlationId()).isEqualTo(CORRELATION_ID);
    }
}
