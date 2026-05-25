/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.router.Response;

import static org.assertj.core.api.Assertions.assertThat;

class RoutingContextImplTest {

    private static final TargetCluster TARGET = new TargetCluster("localhost:9092", Optional.empty());
    private static final int CORRELATION_ID = 7;
    private static final short API_VERSION = 12;
    private static final String SESSION_ID = "sess-1";

    private EmbeddedChannel channel;
    private Map<String, RouteDescriptor> routes;
    private AtomicReference<String> forwardedRoute;
    private AtomicReference<Object> forwardedMsg;
    private SimpleMeterRegistry meterRegistry;
    private AtomicInteger pendingResponseCount;
    private ResponseSequencer responseSequencer;
    private NodeIdMapping nodeIdMapping;
    private int nextRoutingCorrelationId = Integer.MIN_VALUE / 2;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
        routes = Map.of(
                "cluster-route", new RouteDescriptor("cluster-route", TARGET, null, List.of()),
                "router-route", new RouteDescriptor("router-route", null, "nested", List.of()));
        forwardedRoute = new AtomicReference<>();
        forwardedMsg = new AtomicReference<>();
        meterRegistry = new SimpleMeterRegistry();
        pendingResponseCount = new AtomicInteger();
        responseSequencer = new ResponseSequencer(channel);
        nodeIdMapping = new IdentityNodeIdMapping("cluster-route");
    }

    private IntSupplier routingIdAllocator() {
        return () -> nextRoutingCorrelationId++;
    }

    private RoutingContextImpl createContext() {
        return new RoutingContextImpl(
                CORRELATION_ID,
                API_VERSION,
                channel,
                SESSION_ID,
                Subject.anonymous(),
                routes,
                (routeName, msg) -> {
                    forwardedRoute.set(routeName);
                    forwardedMsg.set(msg);
                },
                (virtualNodeId, routeName, msg) -> {
                    forwardedRoute.set(routeName);
                    forwardedMsg.set(msg);
                },
                nodeIdMapping,
                routingIdAllocator(),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount,
                responseSequencer);
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
    void shouldForwardRequestToClusterRoute() {
        var ctx = createContext();
        var header = new RequestHeaderData();
        var body = new FetchRequestData();

        CompletableFuture<Response> future = (CompletableFuture<Response>) ctx.sendRequestToNode("cluster-route", 0, header, body);

        assertThat(forwardedRoute.get()).isEqualTo("cluster-route");
        assertThat(forwardedMsg.get())
                .isInstanceOfSatisfying(DecodedRequestFrame.class, frame -> {
                    assertThat(frame.correlationId()).isNotEqualTo(CORRELATION_ID);
                    assertThat(frame.apiVersion()).isEqualTo(API_VERSION);
                    assertThat(frame.header()).isSameAs(header);
                    assertThat(frame.body()).isSameAs(body);
                });
        assertThat(future).isNotCompleted();
    }

    @Test
    void shouldFailForUnknownRoute() {
        var ctx = createContext();
        var future = ctx.sendRequestToNode("nonexistent", 0, new RequestHeaderData(), new FetchRequestData());

        assertThat(future.toCompletableFuture())
                .isCompletedExceptionally()
                .hasFailedWithThrowableThat()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown route: nonexistent");
    }

    @Test
    void shouldFailForNestedRouterRoute() {
        var ctx = createContext();
        var future = ctx.sendRequestToNode("router-route", 0, new RequestHeaderData(), new FetchRequestData());

        assertThat(future.toCompletableFuture())
                .isCompletedExceptionally()
                .hasFailedWithThrowableThat()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("nested routers");
    }

    @Test
    void shouldReturnBootstrapNodeIdForKnownRoute() {
        var ctx = createContext();
        assertThat(ctx.bootstrapNodeId("cluster-route")).isEqualTo(0);
    }

    @Test
    void shouldThrowForBootstrapNodeIdWithUnknownRoute() {
        var ctx = createContext();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> ctx.bootstrapNodeId("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown route: nonexistent");
    }

    @Test
    void shouldRegisterPendingResponseOnSend() {
        var ctx = createContext();

        ctx.sendRequestToNode("cluster-route", 0, new RequestHeaderData(), new FetchRequestData());

        assertThat(pendingResponseCount.get()).isEqualTo(1);
    }

    @Test
    void shouldIncrementRequestCounterOnSend() {
        var ctx = createContext();
        ctx.sendRequestToNode("cluster-route", 0, new RequestHeaderData(), new FetchRequestData());

        var counter = meterRegistry.find("test_routing_requests").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldIncrementErrorCounterForUnknownRoute() {
        var ctx = createContext();
        ctx.sendRequestToNode("nonexistent", 0, new RequestHeaderData(), new FetchRequestData());

        var counter = meterRegistry.find("test_routing_errors").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldAllocateDistinctRoutingCorrelationIdsForFanOut() {
        var ctx = createContext();

        List<Object> forwardedFrames = new java.util.ArrayList<>();
        var fanOutCtx = new RoutingContextImpl(
                CORRELATION_ID,
                API_VERSION,
                channel,
                SESSION_ID,
                Subject.anonymous(),
                routes,
                (routeName, msg) -> forwardedFrames.add(msg),
                (virtualNodeId, routeName, msg) -> forwardedFrames.add(msg),
                nodeIdMapping,
                routingIdAllocator(),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount,
                responseSequencer);

        var futureA = fanOutCtx.sendRequestToNode("cluster-route", 0, new RequestHeaderData(), new FetchRequestData());
        var futureB = fanOutCtx.sendRequestToNode("cluster-route", 0, new RequestHeaderData(), new FetchRequestData());

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
    void sendRequestToNodeShouldForwardToCorrectRoute() {
        var nodeForwardedId = new AtomicReference<Integer>();
        var nodeForwardedRoute = new AtomicReference<String>();
        var nodeForwardedMsg = new AtomicReference<Object>();

        var ctx = new RoutingContextImpl(
                CORRELATION_ID,
                API_VERSION,
                channel,
                SESSION_ID,
                Subject.anonymous(),
                routes,
                (routeName, msg) -> {
                },
                (virtualNodeId, routeName, msg) -> {
                    nodeForwardedId.set(virtualNodeId);
                    nodeForwardedRoute.set(routeName);
                    nodeForwardedMsg.set(msg);
                },
                nodeIdMapping,
                routingIdAllocator(),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount,
                responseSequencer);

        var header = new RequestHeaderData()
                .setRequestApiKey(org.apache.kafka.common.protocol.ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);
        var future = ctx.sendRequestToNode(0, header, new FetchRequestData());

        assertThat(future.toCompletableFuture()).isNotCompleted();
        assertThat(nodeForwardedId.get()).isEqualTo(0);
        assertThat(nodeForwardedRoute.get()).isEqualTo("cluster-route");
        assertThat(nodeForwardedMsg.get()).isInstanceOf(DecodedRequestFrame.class);
        assertThat(pendingResponseCount.get()).isEqualTo(1);
    }

    @Test
    void sendRequestToNodeShouldFailWhenForwarderThrows() {
        var ctx = new RoutingContextImpl(
                CORRELATION_ID,
                API_VERSION,
                channel,
                SESSION_ID,
                Subject.anonymous(),
                routes,
                (routeName, msg) -> {
                },
                (virtualNodeId, routeName, msg) -> {
                    throw new IllegalStateException("Upstream address not yet known");
                },
                nodeIdMapping,
                routingIdAllocator(),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount,
                responseSequencer);

        var header = new RequestHeaderData()
                .setRequestApiKey(org.apache.kafka.common.protocol.ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);
        var future = ctx.sendRequestToNode(0, header, new FetchRequestData());

        assertThat(future.toCompletableFuture())
                .isCompletedExceptionally();
        assertThat(pendingResponseCount.get())
                .as("pending response should be cleaned up on failure")
                .isZero();
    }
}
