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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import java.util.function.IntUnaryOperator;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.router.Response;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RouterContextImplTest {

    private static final TargetCluster TARGET = new TargetCluster("localhost:9092", Optional.empty());
    private static final int CORRELATION_ID = 7;
    private static final short API_VERSION = 12;
    private static final String SESSION_ID = "sess-1";

    private EmbeddedChannel channel;
    private Map<String, RouteDescriptor> routes;
    private AtomicReference<Integer> forwardedNodeId;
    private AtomicReference<String> forwardedRoute;
    private AtomicReference<Object> forwardedMsg;
    private SimpleMeterRegistry meterRegistry;
    private AtomicInteger pendingResponseCount;
    private ResponseSequencer responseSequencer;
    private NodeIdMapping nodeIdMapping;
    private Map<String, Integer> bootstrapVirtualNodeIds;
    private Map<Integer, HostPort> sharedNodeAddresses;
    private int nextRoutingCorrelationId = Integer.MIN_VALUE / 2;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
        routes = Map.of(
                "cluster-route", new RouteDescriptor("cluster-route", 0, TARGET, null, List.of()),
                "router-route", new RouteDescriptor("router-route", 1, null, "nested", List.of()));
        forwardedNodeId = new AtomicReference<>();
        forwardedRoute = new AtomicReference<>();
        forwardedMsg = new AtomicReference<>();
        meterRegistry = new SimpleMeterRegistry();
        pendingResponseCount = new AtomicInteger();
        responseSequencer = new ResponseSequencer(channel);
        nodeIdMapping = new IdentityNodeIdMapping("cluster-route");
        bootstrapVirtualNodeIds = Map.of("cluster-route", -1);
        sharedNodeAddresses = new HashMap<>();
    }

    private IntSupplier routingIdAllocator() {
        return () -> nextRoutingCorrelationId++;
    }

    private RoutingInfrastructure testInfra() {
        return new RoutingInfrastructure(
                routes, nodeIdMapping, bootstrapVirtualNodeIds,
                routingIdAllocator(),
                Counter.builder("test_routing_requests").withRegistry(meterRegistry),
                Counter.builder("test_routing_errors").withRegistry(meterRegistry),
                Timer.builder("test_routing_duration").withRegistry(meterRegistry),
                pendingResponseCount,
                sharedNodeAddresses,
                IntUnaryOperator.identity());
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
        return new RouterContextImpl(
                clientFrame(),
                channel,
                SESSION_ID,
                Subject.anonymous(),
                testInfra(),
                (routeName, msg) -> {
                    forwardedRoute.set(routeName);
                    forwardedMsg.set(msg);
                },
                (virtualNodeId, routeName, msg) -> {
                    forwardedNodeId.set(virtualNodeId);
                    forwardedRoute.set(routeName);
                    forwardedMsg.set(msg);
                },
                responseSequencer,
                null,
                null);
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
    void shouldReturnBootstrapNodeId() {
        var ctx = createContext();
        assertThat(ctx.bootstrapNodeId("cluster-route")).isEqualTo(-1);
    }

    @Test
    void shouldReturnBootstrapNodeIdForRouterTargetingRoute() {
        var routesWithRouter = Map.of(
                "cluster-route", new RouteDescriptor("cluster-route", 0, TARGET, null, List.of()),
                "router-route", new RouteDescriptor("router-route", 1, null, "nested", List.of()));
        var mapping = new BijectiveNodeIdMapping(Map.of("cluster-route", 0, "router-route", 1), 2);
        var nodeAddresses = new HashMap<Integer, HostPort>();
        var bootstrapIds = RouterContextImpl.computeBootstrapNodeIds(
                routesWithRouter, mapping, nodeAddresses, IntUnaryOperator.identity());

        assertThat(bootstrapIds).containsKey("router-route");
        assertThat(nodeAddresses).as("router-targeting routes should not register addresses")
                .doesNotContainKey(bootstrapIds.get("router-route"));
    }

    @Test
    void shouldThrowForUnknownBootstrapRoute() {
        var ctx = createContext();
        assertThatThrownBy(() -> ctx.bootstrapNodeId("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown route");
    }

    @Test
    void shouldForwardRequestToNode() {
        var ctx = createContext();
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);
        var body = new FetchRequestData();

        var future = ctx.sendRequestToNode("cluster-route", 0, header, body);

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
    void shouldFailForNestedRouterRouteWithoutProvider() {
        var ctx = createContext();
        var future = ctx.sendRequestToNode("router-route", 0,
                new RequestHeaderData().setRequestApiKey(ApiKeys.FETCH.id).setRequestApiVersion(API_VERSION),
                new FetchRequestData());

        assertThat(future.toCompletableFuture())
                .isCompletedExceptionally()
                .hasFailedWithThrowableThat()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Nested routing is not configured");
    }

    @Test
    void shouldSubmitResponseToSequencer() {
        var ctx = createContext();
        var responseHeader = new ResponseHeaderData().setCorrelationId(CORRELATION_ID);
        var responseBody = new FetchResponseData();
        Response response = new ResponseImpl(responseHeader, responseBody);

        ctx.submitResponse(response);

        Object written = channel.readOutbound();
        assertThat(written)
                .isInstanceOfSatisfying(DecodedResponseFrame.class, frame -> {
                    assertThat(frame.correlationId()).isEqualTo(CORRELATION_ID);
                    assertThat(frame.apiVersion()).isEqualTo(API_VERSION);
                    assertThat(frame.header()).isEqualTo(responseHeader);
                    assertThat(frame.body()).isEqualTo(responseBody);
                });
    }

    @Test
    void shouldCloseChannelOnDisconnect() {
        var ctx = createContext();
        assertThat(channel.isOpen()).isTrue();

        ctx.disconnectClient();

        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void shouldRegisterPendingResponseOnSend() {
        var ctx = createContext();
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);

        ctx.sendRequestToNode("cluster-route", 0, header, new FetchRequestData());

        assertThat(pendingResponseCount.get()).isEqualTo(1);
    }

    @Test
    void shouldIncrementRequestCounterOnSend() {
        var ctx = createContext();
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);

        ctx.sendRequestToNode("cluster-route", 0, header, new FetchRequestData());

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
        List<Object> forwardedFrames = new java.util.ArrayList<>();
        var fanOutCtx = new RouterContextImpl(
                clientFrame(),
                channel,
                SESSION_ID,
                Subject.anonymous(),
                testInfra(),
                (routeName, msg) -> forwardedFrames.add(msg),
                (virtualNodeId, routeName, msg) -> forwardedFrames.add(msg),
                responseSequencer,
                null,
                null);

        var headerA = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);
        var headerB = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);

        var futureA = fanOutCtx.sendRequestToNode("cluster-route", 0, headerA, new FetchRequestData());
        var futureB = fanOutCtx.sendRequestToNode("cluster-route", 0, headerB, new FetchRequestData());

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
    void sendRequestToNodeShouldFailWhenForwarderThrows() {
        var ctx = new RouterContextImpl(
                clientFrame(),
                channel,
                SESSION_ID,
                Subject.anonymous(),
                testInfra(),
                (routeName, msg) -> {
                    throw new IllegalStateException("Upstream address not yet known");
                },
                (virtualNodeId, routeName, msg) -> {
                    throw new IllegalStateException("Upstream address not yet known");
                },
                responseSequencer,
                null,
                null);

        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION);
        var future = ctx.sendRequestToNode("cluster-route", 0, header, new FetchRequestData());

        assertThat(future.toCompletableFuture())
                .isCompletedExceptionally();
        assertThat(pendingResponseCount.get())
                .as("pending response should be cleaned up on failure")
                .isZero();
    }
}
