/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.FetchResponseData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.EventExecutor;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.CorrelationIdAllocator;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RouteDispatcherTest {

    private static final String ROUTER_NAME = "nested-router";
    private static final String SESSION_ID = "test-session";
    private static final int CLIENT_CORRELATION_ID = 42;
    private static final int ROUTING_CORRELATION_ID = 100;

    private EmbeddedChannel channel;
    private NodeIdMapping lastCreatedMapping;

    @Mock
    private CorrelationIdAllocator correlationIdAllocator;

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    private static RouteDescriptor clusterRoute(String name, int id) {
        return new RouteDescriptor(name, id, new TargetCluster("broker:9092", null), null, List.of());
    }

    private static RouteDescriptor routerRoute(String name, int id, String routerName) {
        return new RouteDescriptor(name, id, null, routerName, List.of());
    }

    private RouteDispatcher createDispatcher(Map<String, RouteDescriptor> routes, String routePrefix) {
        var capture = new ChannelInboundHandlerAdapter();
        channel = new EmbeddedChannel(capture);
        ChannelHandlerContext ctx = channel.pipeline().context(capture);
        NodeIdMapping mapping = routes.size() == 1
                ? new IdentityNodeIdMapping(routes.keySet().iterator().next())
                : new BijectiveNodeIdMapping(
                        routes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().id())),
                        routes.size());
        lastCreatedMapping = mapping;
        Map<Integer, HostPort> routerNodeAddresses = new HashMap<>();
        var dispatcher = new RouteDispatcher(routes, mapping, routePrefix, correlationIdAllocator, routerNodeAddresses, "test-cluster");
        dispatcher.setContext(ctx);
        return dispatcher;
    }

    private static RequestHeaderData fetchHeader() {
        return new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12);
    }

    private static RequestHeaderData produceHeader() {
        return new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9);
    }

    // --- sendToAnyNode ---

    @Test
    void shouldReturnFailedFutureForUnknownRoute() {
        // Given
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");

        // When
        var future = dispatcher.sendToAnyNode("unknown", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(future.toCompletableFuture()).isCompletedExceptionally();
    }

    @Test
    void shouldFireQualifiedFrameWithPrefix() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");

        // When
        dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired).isNotNull();
        assertThat(fired.routeName()).isEqualTo(ROUTER_NAME + "/r1");
    }

    @Test
    void shouldFireUnqualifiedFrameWithEmptyPrefix() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), "");

        // When
        dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired).isNotNull();
        assertThat(fired.routeName()).isEqualTo("r1");
    }

    @Test
    void shouldRegisterPendingResponseWhenResponseExpected() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");

        // When
        var future = dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(dispatcher.pendingResponseCount()).isEqualTo(1);
        assertThat(future.toCompletableFuture()).isNotDone();
        assertThat(dispatcher.getPendingResponse(ROUTING_CORRELATION_ID).nodeIdMapping()).isSameAs(lastCreatedMapping);
        assertThat(dispatcher.getPendingResponse(ROUTING_CORRELATION_ID).route()).isEqualTo("r1");
    }

    @Test
    void shouldReturnCompletedNullForFireAndForget() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");

        // When
        var future = dispatcher.sendToAnyNode("r1", produceHeader(), new ProduceRequestData().setAcks((short) 0), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(future.toCompletableFuture()).isCompletedWithValue(null);
        assertThat(dispatcher.hasPendingResponses()).isFalse();
    }

    // --- sendToSpecificNode ---

    @Test
    void shouldReturnFailedFutureForUnknownRouteOnSpecificNode() {
        // Given
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");

        // When
        var future = dispatcher.sendToSpecificNode(5, "unknown", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(future.toCompletableFuture()).isCompletedExceptionally();
    }

    @Test
    void shouldSetTargetNodeIdForClusterRoute() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");

        // When
        dispatcher.sendToSpecificNode(5, "r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired).isNotNull();
        assertThat(fired.targetVirtualNodeId()).isEqualTo(5);
        assertThat(fired.routeName()).isEqualTo(ROUTER_NAME + "/r1");
    }

    @Test
    void shouldSetTargetNodeIdForRouterRoute() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("nested", routerRoute("nested", 0, "deeply-nested")), ROUTER_NAME + "/");

        // When
        dispatcher.sendToSpecificNode(5, "nested", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired).isNotNull();
        assertThat(fired.targetVirtualNodeId()).isEqualTo(5);
        assertThat(fired.routeName()).isEqualTo(ROUTER_NAME + "/nested");
    }

    @Test
    void shouldRegisterPendingResponseForSpecificNode() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");

        // When
        var future = dispatcher.sendToSpecificNode(5, "r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(dispatcher.pendingResponseCount()).isEqualTo(1);
        assertThat(future.toCompletableFuture()).isNotDone();
        assertThat(dispatcher.getPendingResponse(ROUTING_CORRELATION_ID).nodeIdMapping()).isSameAs(lastCreatedMapping);
        assertThat(dispatcher.getPendingResponse(ROUTING_CORRELATION_ID).route()).isEqualTo("r1");
    }

    @Test
    void shouldReturnCompletedNullForFireAndForgetSpecificNode() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");

        // When
        var future = dispatcher.sendToSpecificNode(5, "r1", produceHeader(), new ProduceRequestData().setAcks((short) 0), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(future.toCompletableFuture()).isCompletedWithValue(null);
        assertThat(dispatcher.hasPendingResponses()).isFalse();
    }

    // --- event loop confinement ---

    @Test
    void sendToAnyNodeShouldExecuteOnEventLoopWhenCalledFromDifferentThread() throws Exception {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");
        Thread eventLoopThread = obtainEventLoopThread();
        var dispatchThread = new AtomicReference<Thread>();

        // When
        var future = CompletableFuture.supplyAsync(() -> {
            dispatchThread.set(Thread.currentThread());
            return dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);
        });
        channel.runPendingTasks();
        future.get(5, TimeUnit.SECONDS);

        // Then
        assertThat(dispatchThread.get())
                .describedAs("caller must be off the event loop for this test to be meaningful")
                .isNotEqualTo(eventLoopThread);
        assertThat(dispatcher.pendingResponseCount()).isEqualTo(1);
        assertThat(dispatcher.getPendingResponse(ROUTING_CORRELATION_ID)).isNotNull();
        assertThat(dispatcher.getPendingResponse(ROUTING_CORRELATION_ID).nodeIdMapping()).isSameAs(lastCreatedMapping);
        assertThat(dispatcher.getPendingResponse(ROUTING_CORRELATION_ID).route()).isEqualTo("r1");
    }

    @Test
    void sendToSpecificNodeShouldExecuteOnEventLoopWhenCalledFromDifferentThread() throws Exception {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");
        Thread eventLoopThread = obtainEventLoopThread();
        var dispatchThread = new AtomicReference<Thread>();

        // When
        var future = CompletableFuture.supplyAsync(() -> {
            dispatchThread.set(Thread.currentThread());
            return dispatcher.sendToSpecificNode(5, "r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);
        });
        channel.runPendingTasks();
        future.get(5, TimeUnit.SECONDS);

        // Then
        assertThat(dispatchThread.get())
                .describedAs("caller must be off the event loop for this test to be meaningful")
                .isNotEqualTo(eventLoopThread);
        assertThat(dispatcher.pendingResponseCount()).isEqualTo(1);
        assertThat(dispatcher.getPendingResponse(ROUTING_CORRELATION_ID)).isNotNull();
        assertThat(dispatcher.getPendingResponse(ROUTING_CORRELATION_ID).nodeIdMapping()).isSameAs(lastCreatedMapping);
        assertThat(dispatcher.getPendingResponse(ROUTING_CORRELATION_ID).route()).isEqualTo("r1");
    }

    @Test
    void sendToAnyNodeShouldCompleteBridgeExceptionallyIfWorkThrowsWhenOffEventLoop() {
        // Given: an executor that reports off-loop and runs submitted tasks inline (simulates event loop picking up the task)
        var mockExecutor = mock(EventExecutor.class);
        when(mockExecutor.inEventLoop()).thenReturn(false);
        doAnswer(inv -> {
            ((Runnable) inv.getArgument(0)).run();
            return null;
        }).when(mockExecutor).execute(any(Runnable.class));
        var mockCtx = mock(ChannelHandlerContext.class);
        when(mockCtx.executor()).thenReturn(mockExecutor);

        when(correlationIdAllocator.allocateId()).thenThrow(new RuntimeException("allocator failure"));

        var dispatcher = new RouteDispatcher(
                Map.of("r1", clusterRoute("r1", 0)),
                new IdentityNodeIdMapping("r1"),
                ROUTER_NAME + "/",
                correlationIdAllocator,
                new HashMap<>(),
                "test-cluster");
        dispatcher.setContext(mockCtx);

        // When
        var stage = dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(stage.toCompletableFuture()).isCompletedExceptionally();
    }

    @SuppressWarnings({
            // FutureReturnValueIgnored: completion is observed via `future`, asserted immediately below;
            // if the submitted task threw, `future` simply wouldn't complete and that assertion would fail.
            "FutureReturnValueIgnored",
            // The above comment is seen by sonar as commented-out code :facepalm:
            "java:S125" })
    private Thread obtainEventLoopThread() {
        var future = new CompletableFuture<Thread>();
        channel.eventLoop().submit(() -> future.complete(Thread.currentThread()));
        channel.runPendingTasks();
        assertThat(future).isCompleted();
        return future.getNow(null);
    }

    // --- accessors ---

    @Test
    void shouldReturnRoutes() {
        // Given
        var routes = Map.of("r1", clusterRoute("r1", 0));
        var dispatcher = createDispatcher(routes, ROUTER_NAME + "/");

        // When / Then
        assertThat(dispatcher.routes()).isEqualTo(routes);
    }

    @Test
    void shouldReturnNodeIdMapping() {
        // Given
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");

        // When / Then
        assertThat(dispatcher.nodeIdMapping()).isInstanceOf(IdentityNodeIdMapping.class);
    }

    // --- handleResponse ---

    @Test
    void handleResponseShouldReturnConsumedForPendingResponse() {
        // Given
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");
        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        dispatcher.addPendingResponse(ROUTING_CORRELATION_ID, new RouteDispatcher.PendingResponse(future, "r1", lastCreatedMapping));
        var responseFrame = new DecodedResponseFrame<>(
                (short) 12, ROUTING_CORRELATION_ID,
                new ResponseHeaderData(), new FetchResponseData());

        // When
        var outcome = dispatcher.handleResponse(responseFrame, SESSION_ID);

        // Then
        assertThat(outcome).isEqualTo(RouteDispatcher.ResponseOutcome.CONSUMED);
        assertThat(future).isCompleted();
    }

    @Test
    void handleResponseShouldReturnStaticTranslatedForPendingStaticRoute() {
        // Given
        var routes = Map.of("r1", clusterRoute("r1", 0), "r2", clusterRoute("r2", 1));
        var dispatcher = createDispatcher(routes, ROUTER_NAME + "/");
        dispatcher.addPendingStaticRoute(ROUTING_CORRELATION_ID, "r1");
        var metadataResponse = new MetadataResponseData();
        metadataResponse.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(0).setHost("broker1").setPort(9092));
        var responseFrame = new DecodedResponseFrame<>(
                (short) 12, ROUTING_CORRELATION_ID,
                new ResponseHeaderData(), metadataResponse);

        // When
        var outcome = dispatcher.handleResponse(responseFrame, SESSION_ID);

        // Then
        assertThat(outcome).isEqualTo(RouteDispatcher.ResponseOutcome.STATIC_TRANSLATED);
        // BijectiveNodeIdMapping with totalRoutes=2 and r1.id=0: toVirtual("r1", 0) = 0 + 2*0 = 0
        assertThat(metadataResponse.brokers().find(0)).isNotNull();
    }

    @Test
    void handleResponseShouldReturnUnhandledForUnknownCorrelationId() {
        // Given
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");
        var responseFrame = new DecodedResponseFrame<>(
                (short) 12, 999,
                new ResponseHeaderData(), new FetchResponseData());

        // When
        var outcome = dispatcher.handleResponse(responseFrame, SESSION_ID);

        // Then
        assertThat(outcome).isEqualTo(RouteDispatcher.ResponseOutcome.UNHANDLED);
    }

    @Test
    void handleResponseShouldTranslateNodeIdsForPendingResponse() {
        // Given
        var routes = Map.of("r1", clusterRoute("r1", 0), "r2", clusterRoute("r2", 1));
        var dispatcher = createDispatcher(routes, ROUTER_NAME + "/");
        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        dispatcher.addPendingResponse(ROUTING_CORRELATION_ID, new RouteDispatcher.PendingResponse(future, "r1", lastCreatedMapping));
        var metadataResponse = new MetadataResponseData();
        metadataResponse.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(3).setHost("broker1").setPort(9092));
        var responseFrame = new DecodedResponseFrame<>(
                (short) 12, ROUTING_CORRELATION_ID,
                new ResponseHeaderData(), metadataResponse);

        // When
        dispatcher.handleResponse(responseFrame, SESSION_ID);

        // Then
        // BijectiveNodeIdMapping with totalRoutes=2 and r1.id=0: toVirtual("r1", 3) = 0 + 2*3 = 6
        assertThat(metadataResponse.brokers().find(6)).isNotNull();
        assertThat(metadataResponse.brokers().find(3)).isNull();
    }

    @Test
    void handleResponseShouldCacheMetadataAddresses() {
        // Given
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");
        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        dispatcher.addPendingResponse(ROUTING_CORRELATION_ID, new RouteDispatcher.PendingResponse(future, "r1", lastCreatedMapping));
        var metadataResponse = new MetadataResponseData();
        metadataResponse.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(1).setHost("broker1").setPort(9092));
        metadataResponse.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(2).setHost("broker2").setPort(9093));
        var responseFrame = new DecodedResponseFrame<>(
                (short) 12, ROUTING_CORRELATION_ID,
                new ResponseHeaderData(), metadataResponse);

        // When
        dispatcher.handleResponse(responseFrame, SESSION_ID);

        // Then
        assertThat(dispatcher.routerNodeAddresses())
                .containsEntry(1, new HostPort("broker1", 9092))
                .containsEntry(2, new HostPort("broker2", 9093));
    }

    @Test
    void handleResponseShouldCompleteExceptionallyAndReleaseFrameWhenTranslateThrows() {
        // Given
        var routes = Map.of("r1", clusterRoute("r1", 0), "r2", clusterRoute("r2", 1));
        var dispatcher = createDispatcher(routes, ROUTER_NAME + "/");
        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        // Use a route name not in the BijectiveNodeIdMapping to force toVirtual() to throw
        dispatcher.addPendingResponse(ROUTING_CORRELATION_ID, new RouteDispatcher.PendingResponse(future, "unknown-route", lastCreatedMapping));
        var metadataResponse = new MetadataResponseData();
        metadataResponse.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(0).setHost("broker1").setPort(9092));
        var responseFrame = new DecodedResponseFrame<>(
                (short) 12, ROUTING_CORRELATION_ID,
                new ResponseHeaderData(), metadataResponse);

        // When
        var outcome = dispatcher.handleResponse(responseFrame, SESSION_ID);

        // Then
        assertThat(outcome).isEqualTo(RouteDispatcher.ResponseOutcome.CONSUMED);
        assertThat(future).isCompletedExceptionally();
        assertThat(responseFrame.refCnt()).isZero();
    }

    // --- failAllPending ---

    @Test
    void failAllPendingShouldCompleteOutstandingFuturesExceptionally() {
        // Given
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");
        CompletableFuture<ApiMessage> future1 = new CompletableFuture<>();
        CompletableFuture<ApiMessage> future2 = new CompletableFuture<>();
        dispatcher.addPendingResponse(100, new RouteDispatcher.PendingResponse(future1, "r1", lastCreatedMapping));
        dispatcher.addPendingResponse(101, new RouteDispatcher.PendingResponse(future2, "r1", lastCreatedMapping));

        // When
        dispatcher.failAllPending(SESSION_ID);

        // Then
        assertThat(future1).isCompletedExceptionally();
        assertThat(future2).isCompletedExceptionally();
        assertThat(dispatcher.hasPendingResponses()).isFalse();
    }

    // --- qualifyRoute ---

    @Test
    void qualifyRouteShouldApplyPrefix() {
        // Given
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), "prefix/");

        // When
        var qualified = dispatcher.qualifyRoute("route");

        // Then
        assertThat(qualified).isEqualTo("prefix/route");
    }
}
