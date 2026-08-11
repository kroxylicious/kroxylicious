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

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.CorrelationIdAllocator;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
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
        assertThat(dispatcher.pendingResponses).hasSize(1);
        assertThat(future.toCompletableFuture()).isNotDone();
        assertThat(dispatcher.pendingResponses.get(ROUTING_CORRELATION_ID).nodeIdMapping()).isSameAs(lastCreatedMapping);
        assertThat(dispatcher.pendingResponses.get(ROUTING_CORRELATION_ID).route()).isEqualTo("r1");
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
        assertThat(dispatcher.pendingResponses).isEmpty();
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
    void shouldNotSetTargetNodeIdForRouterRoute() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("nested", routerRoute("nested", 0, "deeply-nested")), ROUTER_NAME + "/");

        // When
        dispatcher.sendToSpecificNode(5, "nested", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired).isNotNull();
        assertThat(fired.targetVirtualNodeId()).isEqualTo(Frame.NO_TARGET_VIRTUAL_NODE_ID);
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
        assertThat(dispatcher.pendingResponses).hasSize(1);
        assertThat(future.toCompletableFuture()).isNotDone();
        assertThat(dispatcher.pendingResponses.get(ROUTING_CORRELATION_ID).nodeIdMapping()).isSameAs(lastCreatedMapping);
        assertThat(dispatcher.pendingResponses.get(ROUTING_CORRELATION_ID).route()).isEqualTo("r1");
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
        assertThat(dispatcher.pendingResponses).isEmpty();
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
        assertThat(dispatcher.pendingResponses).hasSize(1);
        assertThat(dispatcher.pendingResponses.get(ROUTING_CORRELATION_ID)).isNotNull();
        assertThat(dispatcher.pendingResponses.get(ROUTING_CORRELATION_ID).nodeIdMapping()).isSameAs(lastCreatedMapping);
        assertThat(dispatcher.pendingResponses.get(ROUTING_CORRELATION_ID).route()).isEqualTo("r1");
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
        assertThat(dispatcher.pendingResponses).hasSize(1);
        assertThat(dispatcher.pendingResponses.get(ROUTING_CORRELATION_ID)).isNotNull();
        assertThat(dispatcher.pendingResponses.get(ROUTING_CORRELATION_ID).nodeIdMapping()).isSameAs(lastCreatedMapping);
        assertThat(dispatcher.pendingResponses.get(ROUTING_CORRELATION_ID).route()).isEqualTo("r1");
    }

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
        dispatcher.pendingResponses.put(ROUTING_CORRELATION_ID, new RouteDispatcher.PendingResponse(future, "r1", lastCreatedMapping));
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
        dispatcher.pendingStaticRoutes.put(ROUTING_CORRELATION_ID, "r1");
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
        dispatcher.pendingResponses.put(ROUTING_CORRELATION_ID, new RouteDispatcher.PendingResponse(future, "r1", lastCreatedMapping));
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
        dispatcher.pendingResponses.put(ROUTING_CORRELATION_ID, new RouteDispatcher.PendingResponse(future, "r1", lastCreatedMapping));
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

    // --- failAllPending ---

    @Test
    void failAllPendingShouldCompleteOutstandingFuturesExceptionally() {
        // Given
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");
        CompletableFuture<ApiMessage> future1 = new CompletableFuture<>();
        CompletableFuture<ApiMessage> future2 = new CompletableFuture<>();
        dispatcher.pendingResponses.put(100, new RouteDispatcher.PendingResponse(future1, "r1", lastCreatedMapping));
        dispatcher.pendingResponses.put(101, new RouteDispatcher.PendingResponse(future2, "r1", lastCreatedMapping));

        // When
        dispatcher.failAllPending(SESSION_ID);

        // Then
        assertThat(future1).isCompletedExceptionally();
        assertThat(future2).isCompletedExceptionally();
        assertThat(dispatcher.pendingResponses).isEmpty();
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
