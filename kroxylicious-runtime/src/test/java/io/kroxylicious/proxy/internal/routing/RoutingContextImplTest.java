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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.routing.Response;

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

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
        routes = Map.of(
                "cluster-route", new RouteDescriptor("cluster-route", TARGET, null, List.of()),
                "router-route", new RouteDescriptor("router-route", null, "nested", List.of()));
        forwardedRoute = new AtomicReference<>();
        forwardedMsg = new AtomicReference<>();
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
                });
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

        CompletableFuture<Response> future = (CompletableFuture<Response>) ctx.sendRequest("cluster-route", header, body);

        assertThat(forwardedRoute.get()).isEqualTo("cluster-route");
        assertThat(forwardedMsg.get())
                .isInstanceOfSatisfying(DecodedRequestFrame.class, frame -> {
                    assertThat(frame.correlationId()).isEqualTo(CORRELATION_ID);
                    assertThat(frame.apiVersion()).isEqualTo(API_VERSION);
                    assertThat(frame.header()).isSameAs(header);
                    assertThat(frame.body()).isSameAs(body);
                });
        assertThat(future).isNotCompleted();
    }

    @Test
    void shouldFailForUnknownRoute() {
        var ctx = createContext();
        var future = ctx.sendRequest("nonexistent", new RequestHeaderData(), new FetchRequestData());

        assertThat(future.toCompletableFuture())
                .isCompletedExceptionally()
                .hasFailedWithThrowableThat()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown route: nonexistent");
    }

    @Test
    void shouldFailForNestedRouterRoute() {
        var ctx = createContext();
        var future = ctx.sendRequest("router-route", new RequestHeaderData(), new FetchRequestData());

        assertThat(future.toCompletableFuture())
                .isCompletedExceptionally()
                .hasFailedWithThrowableThat()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("nested routers");
    }

    @Test
    void shouldWriteResponseToClientChannel() {
        var ctx = createContext();
        var responseHeader = new ResponseHeaderData().setCorrelationId(CORRELATION_ID);
        var responseBody = new FetchResponseData();
        Response response = new ResponseImpl(responseHeader, responseBody);

        ctx.sendResponse(response);

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

        ctx.disconnect();

        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void shouldRegisterPendingResponseOnSend() {
        var ctx = createContext();

        ctx.sendRequest("cluster-route", new RequestHeaderData(), new FetchRequestData());

        CompletableFuture<Response> pendingFuture = new CompletableFuture<>();
        RouterDispatchHandler.registerPendingResponse(channel, 999, pendingFuture);
        assertThat(pendingFuture).isNotCompleted();
    }
}
