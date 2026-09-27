/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.client;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.router.CloseOrTerminalStage;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.router.TerminalStage;
import io.kroxylicious.proxy.topology.VirtualNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings({ "java:S5738", "removal" })
class ClientRoutingRouterTest {

    public static final String REJECTED_COUNTER_NAME = "kroxylicious_client_router_rejected_total";

    private static RequestHeaderData header(String clientId) {
        return new RequestHeaderData().setClientId(clientId);
    }

    private static final MetadataRequestData REQUEST = new MetadataRequestData();
    private static final List<Rule> RULES = List.of(
            new Rule("alice", "team-a"),
            new Rule("bob", "team-b"));

    @Mock
    private RouterContext ctx;

    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void registerMeterRegistry() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
    }

    @AfterEach
    void deregisterMeterRegistry() {
        meterRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.remove(meterRegistry);
    }

    private static TerminalStage terminalStage(RouterResponse response) {
        return new TerminalStage() {
            @Override
            public RouterResponse build() {
                return response;
            }

            @Override
            public CompletionStage<RouterResponse> completed() {
                return CompletableFuture.completedFuture(response);
            }
        };
    }

    private static CloseOrTerminalStage closeableStage(RouterResponse response) {
        TerminalStage closed = terminalStage(response);
        return new CloseOrTerminalStage() {
            @Override
            public TerminalStage withCloseConnection() {
                return closed;
            }

            @Override
            public RouterResponse build() {
                return response;
            }

            @Override
            public CompletionStage<RouterResponse> completed() {
                return CompletableFuture.completedFuture(response);
            }
        };
    }

    private static ApiVersionsResponseData apiVersionsResponse(short errorCode, ApiVersionsResponseData.ApiVersion... versions) {
        ApiVersionsResponseData.ApiVersionCollection collection = new ApiVersionsResponseData.ApiVersionCollection(versions.length);
        collection.addAll(List.of(versions));
        return new ApiVersionsResponseData().setErrorCode(errorCode).setApiKeys(collection);
    }

    private ClientRoutingRouter newRouter() {
        return newRouter(RULES);
    }

    private ClientRoutingRouter newRouter(List<Rule> rules) {
        return new ClientRoutingRouter(rules, "vc1", "subj-router");
    }

    @Test
    void staticRoutesIsEmpty() {
        // Given
        ClientRoutingRouter router = newRouter();

        // When
        var staticRoutes = router.staticRoutes();

        // Then
        assertThat(staticRoutes).isEmpty();
    }

    @Test
    void mappedClientIdForwardsToSelectedRoute() {
        // Given
        RequestHeaderData aliceClientIdHeader = header("alice");
        VirtualNode node = mock(VirtualNode.class);
        when(ctx.anyNode("team-a")).thenReturn(node);
        MetadataResponseData upstreamResponse = new MetadataResponseData();
        when(ctx.sendRequest(node, aliceClientIdHeader, REQUEST)).thenReturn(CompletableFuture.completedFuture(upstreamResponse));
        RouterResponse routerResponse = mock(RouterResponse.class);
        when(ctx.respondWith(upstreamResponse)).thenReturn(closeableStage(routerResponse));

        // When
        RouterResponse result = newRouter().onRequest(ApiKeys.METADATA, (short) 0, aliceClientIdHeader, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(routerResponse);
        verify(ctx).sendRequest(node, aliceClientIdHeader, REQUEST);
    }

    @Test
    void brokerSpecificConnectionForwardsToConnectedNodeNotAnArbitraryOne() {
        // Given
        RequestHeaderData aliceClientIdHeader = header("alice");
        VirtualNode connectedNode = mock(VirtualNode.class);
        when(ctx.virtualNode()).thenReturn(Optional.of(connectedNode));
        MetadataResponseData upstreamResponse = new MetadataResponseData();
        when(ctx.sendRequest(connectedNode, aliceClientIdHeader, REQUEST)).thenReturn(CompletableFuture.completedFuture(upstreamResponse));
        RouterResponse routerResponse = mock(RouterResponse.class);
        when(ctx.respondWith(upstreamResponse)).thenReturn(closeableStage(routerResponse));

        // When
        RouterResponse result = newRouter().onRequest(ApiKeys.METADATA, (short) 0, aliceClientIdHeader, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(routerResponse);
        verify(ctx).sendRequest(connectedNode, aliceClientIdHeader, REQUEST);
        verify(ctx, never()).anyNode(any());
    }

    @Test
    void unmappedRejected() {
        // Given
        RouterResponse errorResponse = mock(RouterResponse.class);
        RequestHeaderData unmappedClientId = header("unmapped");
        when(ctx.respondWithError(unmappedClientId, REQUEST, Errors.POLICY_VIOLATION)).thenReturn(closeableStage(errorResponse));

        // When
        RouterResponse result = newRouter().onRequest(ApiKeys.METADATA, (short) 0, unmappedClientId, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(errorResponse);
    }

    @Test
    void repeatedRequestsForSameRouteAllForward() {
        // Given
        RequestHeaderData aliceHeader = header("alice");
        VirtualNode node = mock(VirtualNode.class);
        when(ctx.anyNode("team-a")).thenReturn(node);
        MetadataResponseData upstreamResponse = new MetadataResponseData();
        when(ctx.sendRequest(node, aliceHeader, REQUEST)).thenReturn(CompletableFuture.completedFuture(upstreamResponse));
        RouterResponse routerResponse = mock(RouterResponse.class);
        when(ctx.respondWith(upstreamResponse)).thenReturn(closeableStage(routerResponse));
        ClientRoutingRouter router = newRouter();
        router.onRequest(ApiKeys.METADATA, (short) 0, aliceHeader, REQUEST, ctx).toCompletableFuture().join();

        // When
        RouterResponse result = router.onRequest(ApiKeys.METADATA, (short) 0, aliceHeader, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(routerResponse);
        verify(ctx, times(2)).sendRequest(node, aliceHeader, REQUEST);
    }

    @Test
    void rejectionIncrementsRejectedCounterWithBoundedReasonTag() {
        // Given
        RequestHeaderData unmatchedClientId = header("unmatched");
        when(ctx.respondWithError(unmatchedClientId, REQUEST, Errors.POLICY_VIOLATION)).thenReturn(closeableStage(mock(RouterResponse.class)));

        // When
        newRouter().onRequest(ApiKeys.METADATA, (short) 0, unmatchedClientId, REQUEST, ctx).toCompletableFuture().join();

        // Then
        assertThat(Metrics.globalRegistry.get(REJECTED_COUNTER_NAME)
                .tags("virtual_cluster", "vc1", "router", "subj-router", "reason", "unmapped")
                .counter().count()).isEqualTo(1);
    }

    @Test
    void unmappedRejectionIncrementsCounterWithUnmappedReasonTag() {
        // Given
        RequestHeaderData unmappedClientId = header("unmapped");
        when(ctx.respondWithError(unmappedClientId, REQUEST, Errors.POLICY_VIOLATION)).thenReturn(closeableStage(mock(RouterResponse.class)));

        // When
        newRouter().onRequest(ApiKeys.METADATA, (short) 0, unmappedClientId, REQUEST, ctx).toCompletableFuture().join();

        // Then
        assertThat(Metrics.globalRegistry.get(REJECTED_COUNTER_NAME)
                .tags("virtual_cluster", "vc1", "router", "subj-router", "reason", "unmapped")
                .counter().count()).isEqualTo(1);
    }

    @Test
    void clientIdOnlyRuleMatchesAnonymousConnection() {
        // Given
        RequestHeaderData header = header("demo-app");
        List<Rule> rules = List.of(new Rule("demo-app", "team-a"));
        VirtualNode node = mock(VirtualNode.class);
        when(ctx.anyNode("team-a")).thenReturn(node);
        MetadataResponseData upstreamResponse = new MetadataResponseData();
        when(ctx.sendRequest(node, header, REQUEST)).thenReturn(CompletableFuture.completedFuture(upstreamResponse));
        RouterResponse routerResponse = mock(RouterResponse.class);
        when(ctx.respondWith(upstreamResponse)).thenReturn(closeableStage(routerResponse));

        // When
        RouterResponse result = newRouter(rules).onRequest(ApiKeys.METADATA, (short) 0, header, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(routerResponse);
        verify(ctx).sendRequest(node, header, REQUEST);
    }

    @Test
    void clientIdOnlyRuleSkipsApiVersionsFanOut() {
        // Given
        RequestHeaderData header = header("demo-app");
        List<Rule> rules = List.of(new Rule("demo-app", "team-a"));
        VirtualNode node = mock(VirtualNode.class);
        when(ctx.anyNode("team-a")).thenReturn(node);
        ApiVersionsResponseData upstreamResponse = apiVersionsResponse((short) 0);
        when(ctx.sendRequest(node, header, REQUEST)).thenReturn(CompletableFuture.completedFuture(upstreamResponse));
        RouterResponse routerResponse = mock(RouterResponse.class);
        when(ctx.respondWith(upstreamResponse)).thenReturn(closeableStage(routerResponse));

        // When
        RouterResponse result = newRouter(rules).onRequest(ApiKeys.API_VERSIONS, (short) 3, header, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(routerResponse);
        verify(ctx, times(1)).sendRequest(any(), any(), any());
        verify(ctx, never()).anyNode("team-b");
    }

    @Test
    void clientIdIsPinnedFromFirstRequestAndNotReReadLater() {
        // Given: the first request's client.id matches a rule and pins the route
        RequestHeaderData firstHeader = header("demo-app");
        List<Rule> rules = List.of(new Rule("demo-app", "team-a"));
        VirtualNode node = mock(VirtualNode.class);
        when(ctx.anyNode("team-a")).thenReturn(node);
        MetadataResponseData upstreamResponse = new MetadataResponseData();
        when(ctx.sendRequest(eq(node), any(RequestHeaderData.class), eq(REQUEST))).thenReturn(CompletableFuture.completedFuture(upstreamResponse));
        RouterResponse routerResponse = mock(RouterResponse.class);
        when(ctx.respondWith(upstreamResponse)).thenReturn(closeableStage(routerResponse));
        ClientRoutingRouter router = newRouter(rules);
        router.onRequest(ApiKeys.METADATA, (short) 0, firstHeader, REQUEST, ctx).toCompletableFuture().join();

        // When: a later request on the same connection carries a different client.id
        RequestHeaderData laterHeader = header("other-app");
        RouterResponse result = router.onRequest(ApiKeys.METADATA, (short) 0, laterHeader, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then: still routed via the pinned "demo-app" client.id, not rejected or re-evaluated against "other-app"
        assertThat(result).isSameAs(routerResponse);
        verify(ctx, times(2)).sendRequest(eq(node), any(RequestHeaderData.class), eq(REQUEST));
    }

}
