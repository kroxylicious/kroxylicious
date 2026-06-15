/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import java.util.function.IntUnaryOperator;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.router.VirtualNode;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RouterContextImplTest {

    private static final TargetCluster TARGET = new TargetCluster("localhost:9092", Optional.empty());
    private static final int CORRELATION_ID = 7;
    private static final short API_VERSION = 12;
    private static final String SESSION_ID = "sess-1";

    private Map<String, RouteDescriptor> routes;
    private AtomicReference<Integer> forwardedNodeId;
    private AtomicReference<String> forwardedRoute;
    private AtomicReference<Object> forwardedMsg;
    private SimpleMeterRegistry meterRegistry;
    private AtomicInteger pendingResponseCount;
    private NodeIdMapping nodeIdMapping;
    private Map<Integer, HostPort> sharedNodeAddresses;
    private OptionalInt virtualNodeId;
    private final PendingResponseRegistry testPendingResponseRegistry = new PendingResponseRegistry() {
        @Override
        public void register(int correlationId, PendingResponse pendingResponse) {
        }

        @Override
        public void deregister(int correlationId) {
        }
    };
    private int nextRoutingCorrelationId = Integer.MIN_VALUE / 2;

    @BeforeEach
    void setUp() {
        routes = Map.of(
                "cluster-route", new RouteDescriptor("cluster-route", 0, TARGET, null, List.of()),
                "router-route", new RouteDescriptor("router-route", 1, null, "nested", List.of()));
        forwardedNodeId = new AtomicReference<>();
        forwardedRoute = new AtomicReference<>();
        forwardedMsg = new AtomicReference<>();
        meterRegistry = new SimpleMeterRegistry();
        pendingResponseCount = new AtomicInteger();
        nodeIdMapping = new IdentityNodeIdMapping("cluster-route");
        sharedNodeAddresses = new HashMap<>();
        virtualNodeId = OptionalInt.empty();
    }

    private IntSupplier routingIdAllocator() {
        return () -> nextRoutingCorrelationId++;
    }

    private DecodedRequestFrame<?> clientFrame() {
        return new DecodedRequestFrame<>(
                API_VERSION, CORRELATION_ID, true,
                new RequestHeaderData()
                        .setRequestApiKey(ApiKeys.FETCH.id)
                        .setRequestApiVersion(API_VERSION)
                        .setCorrelationId(CORRELATION_ID),
                new FetchRequestData());
    }

    private RouterContextImpl createContext() {
        return createContext(virtualNodeId);
    }

    private RouterContextImpl createContext(OptionalInt virtualNodeId) {
        return new RouterContextImpl(
                CORRELATION_ID,
                SESSION_ID,
                Subject.anonymous(),
                routes,
                (routeName, msg) -> {
                    forwardedRoute.set(routeName);
                    forwardedMsg.set(msg);
                },
                (vnId, routeName, msg) -> {
                    forwardedNodeId.set(vnId);
                    forwardedRoute.set(routeName);
                    forwardedMsg.set(msg);
                },
                nodeIdMapping,
                virtualNodeId,
                routingIdAllocator(),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount,
                testPendingResponseRegistry,
                sharedNodeAddresses,
                IntUnaryOperator.identity(),
                java.util.Map.of());
    }

    @Test
    void shouldReturnSessionId() {
        var ctx = createContext();
        assertThat(ctx.sessionId()).isEqualTo(SESSION_ID);
    }

    @Test
    void shouldReturnAnonymousSubject() {
        var ctx = createContext();
        assertThat(ctx.authenticatedSubject()).isEqualTo(Subject.anonymous());
    }

    @Test
    void shouldReturnEmptyVirtualNodeForBootstrapContext() {
        var ctx = createContext(OptionalInt.empty());
        assertThat(ctx.virtualNode()).isEmpty();
    }

    @Test
    void shouldReturnPresentVirtualNodeForBrokerContext() {
        var ctx = createContext(OptionalInt.of(0));
        assertThat(ctx.virtualNode()).isPresent();
        assertThat(ctx.virtualNode().get()).isEqualTo(new VirtualNodeImpl(0));
    }

    @Test
    void shouldComputeAnyNodeForKnownRoute() {
        var ctx = createContext();
        VirtualNode node = ctx.anyNode("cluster-route");
        assertThat(node).isEqualTo(new VirtualNodeImpl(-1));
    }

    @Test
    void shouldThrowForUnknownRouteInAnyNode() {
        var ctx = createContext();
        assertThatThrownBy(() -> ctx.anyNode("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown route");
    }

    @Test
    void shouldConvertNodeIdToVirtualNode() {
        var ctx = createContext();
        VirtualNode node = ctx.nodeForId(42);
        assertThat(node).isEqualTo(new VirtualNodeImpl(42));
    }

    @Test
    void shouldRegisterBootstrapAddressesForClusterRoutes() {
        var routesWithRouter = Map.of(
                "cluster-route", new RouteDescriptor("cluster-route", 0, TARGET, null, List.of()),
                "router-route", new RouteDescriptor("router-route", 1, null, "nested", List.of()));
        var mapping = new BijectiveNodeIdMapping(Map.of("cluster-route", 0, "router-route", 1), 2);
        var nodeAddresses = new HashMap<Integer, HostPort>();

        // When
        RouterContextImpl.registerBootstrapAddresses(
                routesWithRouter, mapping, nodeAddresses, IntUnaryOperator.identity());

        // Then
        int clusterBootstrapId = mapping.toVirtual("cluster-route", RouterContextImpl.BOOTSTRAP_TARGET_NODE_ID);
        assertThat(nodeAddresses).containsKey(clusterBootstrapId);

        int routerBootstrapId = mapping.toVirtual("router-route", RouterContextImpl.BOOTSTRAP_TARGET_NODE_ID);
        assertThat(nodeAddresses)
                .as("router-targeting routes should not register addresses")
                .doesNotContainKey(routerBootstrapId);
    }

    @Test
    void shouldForwardNonBootstrapRequestViaNodeForwarder() {
        var ctx = createContext();
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);
        var body = new FetchRequestData();

        var future = ctx.sendRequest(ctx.nodeForId(0), header, body);

        assertThat(forwardedNodeId.get()).isEqualTo(0);
        assertThat(forwardedRoute.get()).isEqualTo("cluster-route");
        assertThat(forwardedMsg.get())
                .isInstanceOfSatisfying(DecodedRequestFrame.class, frame -> {
                    assertThat(frame.correlationId()).isNotEqualTo(CORRELATION_ID);
                    assertThat(frame.apiVersion()).isEqualTo(API_VERSION);
                    assertThat(frame.header()).isSameAs(header);
                    assertThat(frame.body()).isSameAs(body);
                });
        assertThat(future.toCompletableFuture()).isNotCompleted();
    }

    @Test
    void shouldForwardBootstrapRequestViaRouteForwarder() {
        var ctx = createContext();
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);
        var body = new FetchRequestData();

        ctx.sendRequest(ctx.anyNode("cluster-route"), header, body);

        assertThat(forwardedRoute.get()).isEqualTo("cluster-route");
        assertThat(forwardedNodeId.get())
                .as("bootstrap should use routeForwarder, not nodeForwarder")
                .isNull();
        assertThat(forwardedMsg.get()).isNotNull();
    }

    @Test
    void shouldDeriveRouteFromVirtualNodeWithBijectiveMapping() {
        // Given: two routes with bijective mapping
        var bijectiveRoutes = Map.of(
                "route-a", new RouteDescriptor("route-a", 0, TARGET, null, List.of()),
                "route-b", new RouteDescriptor("route-b", 1, TARGET, null, List.of()));
        var mapping = new BijectiveNodeIdMapping(Map.of("route-a", 0, "route-b", 1), 2);
        var ctx = new RouterContextImpl(
                CORRELATION_ID, SESSION_ID, Subject.anonymous(),
                bijectiveRoutes,
                (routeName, msg) -> {
                    forwardedRoute.set(routeName);
                    forwardedMsg.set(msg);
                },
                (vnId, routeName, msg) -> {
                    forwardedNodeId.set(vnId);
                    forwardedRoute.set(routeName);
                    forwardedMsg.set(msg);
                },
                mapping, OptionalInt.empty(),
                routingIdAllocator(),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount, testPendingResponseRegistry,
                sharedNodeAddresses, IntUnaryOperator.identity(), Map.of());
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);

        // When: send to virtual node 3 (= route-b, target 1: V = 1 + 2*1 = 3)
        ctx.sendRequest(ctx.nodeForId(3), header, new FetchRequestData());

        // Then: route should be derived as "route-b"
        assertThat(forwardedRoute.get()).isEqualTo("route-b");
        assertThat(forwardedNodeId.get()).isEqualTo(3);
    }

    @Test
    void shouldFailForUnknownRouteInSendRequest() {
        // IdentityNodeIdMapping returns ("cluster-route", virtualNodeId) for any ID,
        // so we need a bijective mapping that can produce an unknown route
        var singleRoute = Map.of(
                "known-route", new RouteDescriptor("known-route", 0, TARGET, null, List.of()));
        var mapping = new BijectiveNodeIdMapping(Map.of("known-route", 0, "phantom", 1), 2);
        var ctx = new RouterContextImpl(
                CORRELATION_ID, SESSION_ID, Subject.anonymous(),
                singleRoute,
                (routeName, msg) -> {
                },
                (vnId, routeName, msg) -> {
                },
                mapping, OptionalInt.empty(),
                routingIdAllocator(),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount, testPendingResponseRegistry,
                sharedNodeAddresses, IntUnaryOperator.identity(), Map.of());

        // Virtual node 1 maps to route "phantom" which isn't in the routes map
        var future = ctx.sendRequest(ctx.nodeForId(1), new RequestHeaderData(), new FetchRequestData());

        assertThat(future.toCompletableFuture())
                .isCompletedExceptionally()
                .hasFailedWithThrowableThat()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown route: phantom");
    }

    @Test
    void shouldRegisterPendingResponseOnSend() {
        var ctx = createContext();
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);

        ctx.sendRequest(ctx.nodeForId(0), header, new FetchRequestData());

        assertThat(pendingResponseCount.get()).isEqualTo(1);
    }

    @Test
    void shouldIncrementRequestCounterOnSend() {
        var ctx = createContext();
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);

        ctx.sendRequest(ctx.nodeForId(0), header, new FetchRequestData());

        var counter = meterRegistry.find("test_routing_requests").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldIncrementErrorCounterForUnknownRoute() {
        // Use bijective mapping so we can produce an unknown route
        var mapping = new BijectiveNodeIdMapping(Map.of("cluster-route", 0, "phantom", 1), 2);
        var ctx = new RouterContextImpl(
                CORRELATION_ID, SESSION_ID, Subject.anonymous(),
                routes,
                (routeName, msg) -> {
                },
                (vnId, routeName, msg) -> {
                },
                mapping, OptionalInt.empty(),
                routingIdAllocator(),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount, testPendingResponseRegistry,
                sharedNodeAddresses, IntUnaryOperator.identity(), Map.of());

        // Virtual node 1 maps to "phantom" which is unknown to routes
        ctx.sendRequest(ctx.nodeForId(1), new RequestHeaderData(), new FetchRequestData());

        var counter = meterRegistry.find("test_routing_errors").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldAllocateDistinctRoutingCorrelationIdsForFanOut() {
        List<Object> forwardedFrames = new java.util.ArrayList<>();
        var fanOutCtx = new RouterContextImpl(
                CORRELATION_ID, SESSION_ID, Subject.anonymous(),
                routes,
                (routeName, msg) -> forwardedFrames.add(msg),
                (vnId, routeName, msg) -> forwardedFrames.add(msg),
                nodeIdMapping, OptionalInt.empty(),
                routingIdAllocator(),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount, testPendingResponseRegistry,
                sharedNodeAddresses, IntUnaryOperator.identity(), Map.of());

        var headerA = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);
        var headerB = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);

        var futureA = fanOutCtx.sendRequest(fanOutCtx.nodeForId(0), headerA, new FetchRequestData());
        var futureB = fanOutCtx.sendRequest(fanOutCtx.nodeForId(0), headerB, new FetchRequestData());

        assertThat(forwardedFrames).hasSize(2);

        int idA = ((DecodedRequestFrame<?>) forwardedFrames.get(0)).correlationId();
        int idB = ((DecodedRequestFrame<?>) forwardedFrames.get(1)).correlationId();
        assertThat(idA).isNotEqualTo(idB);
        assertThat(idA).isNotEqualTo(CORRELATION_ID);
        assertThat(idB).isNotEqualTo(CORRELATION_ID);

        assertThat(pendingResponseCount.get()).isEqualTo(2);
        assertThat(futureA.toCompletableFuture()).isNotCompleted();
        assertThat(futureB.toCompletableFuture()).isNotCompleted();
    }

    @Test
    void sendRequestShouldFailWhenForwarderThrows() {
        var ctx = new RouterContextImpl(
                CORRELATION_ID, SESSION_ID, Subject.anonymous(),
                routes,
                (routeName, msg) -> {
                    throw new IllegalStateException("Upstream address not yet known");
                },
                (vnId, routeName, msg) -> {
                    throw new IllegalStateException("Upstream address not yet known");
                },
                nodeIdMapping, OptionalInt.empty(),
                routingIdAllocator(),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount, testPendingResponseRegistry,
                sharedNodeAddresses, IntUnaryOperator.identity(), Map.of());

        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);
        var future = ctx.sendRequest(ctx.nodeForId(0), header, new FetchRequestData());

        assertThat(future.toCompletableFuture())
                .isCompletedExceptionally();
        assertThat(pendingResponseCount.get())
                .as("pending response should be cleaned up on failure")
                .isZero();
    }
}
