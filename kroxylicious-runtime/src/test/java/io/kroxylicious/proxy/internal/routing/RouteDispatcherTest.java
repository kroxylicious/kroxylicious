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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.EventExecutor;

import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.FetchResponseData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.PathElement;
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

    private RouteDispatcher createDispatcher(Map<String, RouteDescriptor> routes, String qualificationPrefix) {
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
        var dispatcher = new RouteDispatcher(routes, mapping, qualificationPrefix, null, correlationIdAllocator, routerNodeAddresses, "test-cluster");
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
        assertThat(fired.path()).isInstanceOfSatisfying(PathElement.Router.class,
                router -> assertThat(router.next()).isEqualTo(new PathElement.Route(ROUTER_NAME + "/r1", null)));
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
        assertThat(fired.path()).isInstanceOfSatisfying(PathElement.Router.class,
                router -> assertThat(router.next()).isEqualTo(new PathElement.Route("r1", null)));
    }

    @Test
    void shouldRegisterPendingResponseWhenResponseExpected() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");

        // When
        var future = dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(future.toCompletableFuture()).isNotDone();
        DecodedRequestFrame<?> fired = channel.readInbound();
        var responseFrame = new DecodedResponseFrame<>((short) 12, ROUTING_CORRELATION_ID, new ResponseHeaderData(), new FetchResponseData());
        responseFrame.setPath(fired.path());
        assertThat(dispatcher.handleResponse(responseFrame, SESSION_ID)).isEqualTo(RouteDispatcher.ResponseOutcome.CONSUMED);
        assertThat(future.toCompletableFuture()).isCompleted();
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
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired.path())
                .as("no response expected, so no promise-bearing leaf is attached")
                .isEqualTo(new PathElement.Route(ROUTER_NAME + "/r1", null));
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
        assertThat(fired.path()).isInstanceOfSatisfying(PathElement.Router.class,
                router -> assertThat(router.next()).isEqualTo(new PathElement.Route(ROUTER_NAME + "/r1", null)));
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
        assertThat(fired.path()).isInstanceOfSatisfying(PathElement.Router.class,
                router -> assertThat(router.next()).isEqualTo(new PathElement.Route(ROUTER_NAME + "/nested", null)));
    }

    @Test
    void shouldRegisterPendingResponseForSpecificNode() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");

        // When
        var future = dispatcher.sendToSpecificNode(5, "r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // Then
        assertThat(future.toCompletableFuture()).isNotDone();
        DecodedRequestFrame<?> fired = channel.readInbound();
        var responseFrame = new DecodedResponseFrame<>((short) 12, ROUTING_CORRELATION_ID, new ResponseHeaderData(), new FetchResponseData());
        responseFrame.setPath(fired.path());
        assertThat(dispatcher.handleResponse(responseFrame, SESSION_ID)).isEqualTo(RouteDispatcher.ResponseOutcome.CONSUMED);
        assertThat(future.toCompletableFuture()).isCompleted();
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
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired).isNotNull();
        assertThat(fired.path()).isInstanceOf(PathElement.Router.class);
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
        DecodedRequestFrame<?> fired = channel.readInbound();
        assertThat(fired).isNotNull();
        assertThat(fired.path()).isInstanceOf(PathElement.Router.class);
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
                null,
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
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");
        var future = dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);
        DecodedRequestFrame<?> fired = channel.readInbound();
        var responseFrame = new DecodedResponseFrame<>(
                (short) 12, ROUTING_CORRELATION_ID,
                new ResponseHeaderData(), new FetchResponseData());
        responseFrame.setPath(fired.path());

        // When
        var outcome = dispatcher.handleResponse(responseFrame, SESSION_ID);

        // Then
        assertThat(outcome).isEqualTo(RouteDispatcher.ResponseOutcome.CONSUMED);
        assertThat(future.toCompletableFuture()).isCompleted();
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
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(routes, ROUTER_NAME + "/");
        dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);
        DecodedRequestFrame<?> fired = channel.readInbound();
        var metadataResponse = new MetadataResponseData();
        metadataResponse.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(3).setHost("broker1").setPort(9092));
        var responseFrame = new DecodedResponseFrame<>(
                (short) 12, ROUTING_CORRELATION_ID,
                new ResponseHeaderData(), metadataResponse);
        responseFrame.setPath(fired.path());

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
        when(correlationIdAllocator.allocateId()).thenReturn(ROUTING_CORRELATION_ID);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");
        dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);
        DecodedRequestFrame<?> fired = channel.readInbound();
        var metadataResponse = new MetadataResponseData();
        metadataResponse.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(1).setHost("broker1").setPort(9092));
        metadataResponse.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(2).setHost("broker2").setPort(9093));
        var responseFrame = new DecodedResponseFrame<>(
                (short) 12, ROUTING_CORRELATION_ID,
                new ResponseHeaderData(), metadataResponse);
        responseFrame.setPath(fired.path());

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
        // A route name not known to the BijectiveNodeIdMapping, to force toVirtual() to throw.
        var responsePath = new PathElement.Router(future, dispatcher.routePathFor("unknown-route"));
        var metadataResponse = new MetadataResponseData();
        metadataResponse.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(0).setHost("broker1").setPort(9092));
        var responseFrame = new DecodedResponseFrame<>(
                (short) 12, ROUTING_CORRELATION_ID,
                new ResponseHeaderData(), metadataResponse);
        responseFrame.setPath(responsePath);

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
        when(correlationIdAllocator.allocateId()).thenReturn(100, 101);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");
        var future1 = dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);
        var future2 = dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);

        // When
        dispatcher.failAllPending(SESSION_ID);

        // Then
        assertThat(future1.toCompletableFuture()).isCompletedExceptionally();
        assertThat(future2.toCompletableFuture()).isCompletedExceptionally();
    }

    // --- concurrent same-route dispatch ---

    /**
     * Response matching relies entirely on the exact {@link PathElement.Router} leaf (and the
     * {@code CompletableFuture} it carries) round-tripping on the response frame - there is no
     * correlation-id-keyed lookup table backing it. This proves that guarantee holds when the same
     * router has two sends to the same route concurrently in flight: a response addressed to one
     * send's own path must not complete the other, concurrently in-flight, send's future.
     */
    @Test
    void concurrentSendsToSameRouteShouldOnlyCompleteTheMatchingFuture() {
        // Given
        when(correlationIdAllocator.allocateId()).thenReturn(100, 101);
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), ROUTER_NAME + "/");
        var future1 = dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);
        var future2 = dispatcher.sendToAnyNode("r1", fetchHeader(), new FetchRequestData(), SESSION_ID, CLIENT_CORRELATION_ID);
        channel.readInbound(); // the first send's own request frame; its response is deliberately withheld
        DecodedRequestFrame<?> fired2 = channel.readInbound();
        var response2 = new DecodedResponseFrame<>((short) 12, 101, new ResponseHeaderData(), new FetchResponseData());
        response2.setPath(fired2.path());

        // When
        var outcome = dispatcher.handleResponse(response2, SESSION_ID);

        // Then
        assertThat(outcome).isEqualTo(RouteDispatcher.ResponseOutcome.CONSUMED);
        assertThat(future2.toCompletableFuture())
                .as("the response carrying the second send's own path must complete that send's own future")
                .isCompleted();
        assertThat(future1.toCompletableFuture())
                .as("without a correlation-id-keyed lookup table, the first send's future must not be completed "
                        + "by a response addressed to a different, concurrently in-flight send to the same route")
                .isNotDone();
    }

    // --- routePathFor ---

    @Test
    void routePathForShouldApplyPrefix() {
        // Given
        var dispatcher = createDispatcher(Map.of("r1", clusterRoute("r1", 0)), "prefix/");

        // When
        var routePath = dispatcher.routePathFor("route");

        // Then
        assertThat(routePath).isEqualTo(new PathElement.Route("prefix/route", null));
    }
}
