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
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.router.RouterResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RoutingContextImplTest {

    private static final TargetCluster TARGET = new TargetCluster("localhost:9092", Optional.empty());
    private static final int CORRELATION_ID = 7;
    private static final short API_VERSION = 12;
    private static final String SESSION_ID = "sess-1";

    private EmbeddedChannel channel;
    private Map<String, RouteDescriptor> routes;
    private AtomicReference<Object> forwarded;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
        routes = Map.of(
                "cluster-route", new RouteDescriptor("cluster-route", 0, TARGET, null, List.of()),
                "router-route", new RouteDescriptor("router-route", 1, null, "nested", List.of()));
        forwarded = new AtomicReference<>();
    }

    private RoutingContextImpl createContext() {
        return new RoutingContextImpl(
                CORRELATION_ID,
                API_VERSION,
                channel,
                SESSION_ID,
                Subject.anonymous(),
                routes,
                forwarded::set);
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
    void shouldReturnEmptyVirtualNode() {
        var ctx = createContext();
        assertThat(ctx.virtualNode()).isEmpty();
    }

    @Test
    void shouldReturnNodeForKnownRoute() {
        var ctx = createContext();
        var node = ctx.anyNode("cluster-route");
        assertThat(node).isNotNull();
    }

    @Test
    void shouldThrowForAnyNodeWithUnknownRoute() {
        var ctx = createContext();
        assertThatThrownBy(() -> ctx.anyNode("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown route: nonexistent");
    }

    @Test
    void shouldForwardRequestToClusterRoute() {
        var ctx = createContext();
        var header = new RequestHeaderData();
        var body = new FetchRequestData();
        var node = ctx.anyNode("cluster-route");

        CompletableFuture<ApiMessage> future = (CompletableFuture<ApiMessage>) ctx.sendRequest(node, header, body);

        assertThat(forwarded.get())
                .isInstanceOfSatisfying(DecodedRequestFrame.class, frame -> {
                    assertThat(frame.correlationId()).isEqualTo(CORRELATION_ID);
                    assertThat(frame.apiVersion()).isEqualTo(API_VERSION);
                    assertThat(frame.header()).isSameAs(header);
                    assertThat(frame.body()).isSameAs(body);
                });
        assertThat(future).isNotCompleted();
    }

    @Test
    void shouldFailForNestedRouterRoute() {
        var ctx = createContext();
        var node = ctx.anyNode("router-route");
        var future = ctx.sendRequest(node, new RequestHeaderData(), new FetchRequestData());

        assertThat(future.toCompletableFuture()).isCompletedExceptionally();
        assertThatThrownBy(() -> future.toCompletableFuture().join())
                .hasCauseInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("nested routers");
    }

    @Test
    void shouldBuildRespondWithBody() {
        var ctx = createContext();
        var body = new FetchResponseData();

        RouterResponse result = ctx.respondWith(body).build();

        assertThat(result).isInstanceOfSatisfying(RouterResultImpl.class, impl -> {
            assertThat(impl.body()).isSameAs(body);
            assertThat(impl.header()).isNull();
            assertThat(impl.closeConnection()).isFalse();
        });
    }

    @Test
    void shouldBuildRespondWithHeaderAndBody() {
        var ctx = createContext();
        var header = new ResponseHeaderData();
        var body = new FetchResponseData();

        RouterResponse result = ctx.respondWith(header, body).build();

        assertThat(result).isInstanceOfSatisfying(RouterResultImpl.class, impl -> {
            assertThat(impl.header()).isSameAs(header);
            assertThat(impl.body()).isSameAs(body);
            assertThat(impl.closeConnection()).isFalse();
        });
    }

    @Test
    void shouldBuildRespondWithoutReply() {
        var ctx = createContext();

        RouterResponse result = ctx.respondWithoutReply().build();

        assertThat(result).isInstanceOfSatisfying(RouterResultImpl.class, impl -> {
            assertThat(impl.hasBody()).isFalse();
            assertThat(impl.closeConnection()).isFalse();
        });
    }

    @Test
    void shouldBuildWithCloseConnection() {
        var ctx = createContext();

        RouterResponse result = ctx.respondWithoutReply().withCloseConnection().build();

        assertThat(result).isInstanceOfSatisfying(RouterResultImpl.class, impl -> {
            assertThat(impl.closeConnection()).isTrue();
        });
    }

    @Test
    void shouldRegisterPendingResponseOnSend() {
        var ctx = createContext();
        var node = ctx.anyNode("cluster-route");

        ctx.sendRequest(node, new RequestHeaderData(), new FetchRequestData());

        CompletableFuture<ApiMessage> pendingFuture = new CompletableFuture<>();
        RouterDispatchHandler.registerPendingResponse(channel, 999, pendingFuture);
        assertThat(pendingFuture).isNotCompleted();
    }
}
