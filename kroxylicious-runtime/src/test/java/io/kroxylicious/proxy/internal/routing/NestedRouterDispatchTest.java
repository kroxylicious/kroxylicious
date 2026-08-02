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

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
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
import io.kroxylicious.proxy.internal.CorrelationIdAllocator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NestedRouterDispatchTest {

    private static final String ROUTER_NAME = "nested-router";
    private static final String SESSION_ID = "test-session";
    private static final int CLIENT_CORRELATION_ID = 42;
    private static final int ROUTING_CORRELATION_ID = 100;

    private EmbeddedChannel channel;
    private final Map<Integer, RouterDispatchHandler.PendingResponse> pendingResponses = new HashMap<>();

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

    private NestedRouterDispatch createDispatch(Map<String, RouteDescriptor> routes) {
        var capture = new ChannelInboundHandlerAdapter();
        channel = new EmbeddedChannel(capture);
        ChannelHandlerContext ctx = channel.pipeline().context(capture);
        NodeIdMapping mapping = routes.size() == 1
                ? new IdentityNodeIdMapping(routes.keySet().iterator().next())
                : new BijectiveNodeIdMapping(
                        routes.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().id())),
                        routes.size());
        return new NestedRouterDispatch(routes, mapping, ROUTER_NAME, correlationIdAllocator, pendingResponses, ctx);
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
        var dispatch = createDispatch(Map.of("r1", clusterRoute("r1", 0)));

        // When
        var future = dispatch.sendToAnyNode("unknown", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(future.toCompletableFuture()).isCompletedExceptionally();
    }

    @Test
    void shouldFireQualifiedFrameForRouterTargetingRoute() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var routes = Map.of("nested", routerRoute("nested", 0, "deeply-nested"));
        var dispatch = createDispatch(routes);

        // When
        dispatch.sendToAnyNode("nested", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired).isNotNull();
        assertThat(fired.routeName()).isEqualTo(ROUTER_NAME + "/nested");
    }

    @Test
    void shouldFireQualifiedFrameForClusterRoute() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatch = createDispatch(Map.of("r1", clusterRoute("r1", 0)));

        // When
        dispatch.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired).isNotNull();
        assertThat(fired.routeName()).isEqualTo(ROUTER_NAME + "/r1");
    }

    @Test
    void shouldRegisterPendingResponseWhenResponseExpected() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatch = createDispatch(Map.of("r1", clusterRoute("r1", 0)));

        // When
        var future = dispatch.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(pendingResponses).hasSize(1);
        assertThat(future.toCompletableFuture()).isNotDone();
    }

    @Test
    void shouldReturnCompletedNullForFireAndForget() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatch = createDispatch(Map.of("r1", clusterRoute("r1", 0)));

        // When
        var future = dispatch.sendToAnyNode("r1", produceHeader(), new ProduceRequestData().setAcks((short) 0), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(future.toCompletableFuture()).isCompletedWithValue(null);
        assertThat(pendingResponses).isEmpty();
    }

    // --- sendToSpecificNode ---

    @Test
    void shouldReturnFailedFutureForUnknownRouteOnSpecificNode() {
        // Given
        var dispatch = createDispatch(Map.of("r1", clusterRoute("r1", 0)));

        // When
        var future = dispatch.sendToSpecificNode(5, "unknown", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(future.toCompletableFuture()).isCompletedExceptionally();
    }

    @Test
    void shouldFireFrameWithTargetNodeIdForRouterTargetingRoute() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var routes = Map.of("nested", routerRoute("nested", 0, "deeply-nested"));
        var dispatch = createDispatch(routes);

        // When
        dispatch.sendToSpecificNode(5, "nested", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired).isNotNull();
        assertThat(fired.targetVirtualNodeId()).isEqualTo(5);
        assertThat(fired.routeName()).isEqualTo(ROUTER_NAME + "/nested");
    }

    @Test
    void shouldFireFrameWithTargetNodeId() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatch = createDispatch(Map.of("r1", clusterRoute("r1", 0)));

        // When
        dispatch.sendToSpecificNode(5, "r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired).isNotNull();
        assertThat(fired.targetVirtualNodeId()).isEqualTo(5);
        assertThat(fired.routeName()).isEqualTo(ROUTER_NAME + "/r1");
    }

    @Test
    void shouldRegisterPendingResponseForSpecificNode() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatch = createDispatch(Map.of("r1", clusterRoute("r1", 0)));

        // When
        var future = dispatch.sendToSpecificNode(5, "r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(pendingResponses).hasSize(1);
        assertThat(future.toCompletableFuture()).isNotDone();
    }

    @Test
    void shouldReturnCompletedNullForFireAndForgetSpecificNode() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatch = createDispatch(Map.of("r1", clusterRoute("r1", 0)));

        // When
        var future = dispatch.sendToSpecificNode(5, "r1", produceHeader(), new ProduceRequestData().setAcks((short) 0), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(future.toCompletableFuture()).isCompletedWithValue(null);
        assertThat(pendingResponses).isEmpty();
    }

    // --- event loop confinement ---

    @Test
    void sendToAnyNodeShouldExecuteOnEventLoopWhenCalledFromDifferentThread() throws Exception {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatch = createDispatch(Map.of("r1", clusterRoute("r1", 0)));
        Thread eventLoopThread = obtainEventLoopThread();
        var dispatchThread = new AtomicReference<Thread>();

        // When — supplyAsync runs the call on a ForkJoinPool thread, not the event loop
        var future = CompletableFuture.supplyAsync(() -> {
            dispatchThread.set(Thread.currentThread());
            return dispatch.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);
        });
        channel.runPendingTasks();
        future.get(5, TimeUnit.SECONDS);

        // Then
        assertThat(dispatchThread.get())
                .describedAs("caller must be off the event loop for this test to be meaningful")
                .isNotEqualTo(eventLoopThread);
        assertThat(pendingResponses).hasSize(1);
        assertThat(pendingResponses.get(ROUTING_CORRELATION_ID)).isNotNull();
    }

    @Test
    void sendToSpecificNodeShouldExecuteOnEventLoopWhenCalledFromDifferentThread() throws Exception {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatch = createDispatch(Map.of("r1", clusterRoute("r1", 0)));
        Thread eventLoopThread = obtainEventLoopThread();
        var dispatchThread = new AtomicReference<Thread>();

        // When — supplyAsync runs the call on a ForkJoinPool thread, not the event loop
        var future = CompletableFuture.supplyAsync(() -> {
            dispatchThread.set(Thread.currentThread());
            return dispatch.sendToSpecificNode(5, "r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);
        });
        channel.runPendingTasks();
        future.get(5, TimeUnit.SECONDS);

        // Then
        assertThat(dispatchThread.get())
                .describedAs("caller must be off the event loop for this test to be meaningful")
                .isNotEqualTo(eventLoopThread);
        assertThat(pendingResponses).hasSize(1);
        assertThat(pendingResponses.get(ROUTING_CORRELATION_ID)).isNotNull();
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
        var dispatch = createDispatch(routes);

        // When / Then
        assertThat(dispatch.routes()).isEqualTo(routes);
    }

    @Test
    void shouldReturnNodeIdMapping() {
        // Given
        var dispatch = createDispatch(Map.of("r1", clusterRoute("r1", 0)));

        // When / Then
        assertThat(dispatch.nodeIdMapping()).isInstanceOf(IdentityNodeIdMapping.class);
    }
}
