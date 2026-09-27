/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
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
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.router.CloseOrTerminalStage;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.router.TerminalStage;
import io.kroxylicious.proxy.topology.VirtualNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings({ "java:S5738", "removal" })
class SubjectRoutingHandlerTest {

    private static final RequestHeaderData HEADER = new RequestHeaderData();
    private static final MetadataRequestData REQUEST = new MetadataRequestData();

    @Mock
    private RouterContext ctx;

    @Mock
    private RouteSelector<Object> selector;

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

    private static ApiVersionsResponseData.ApiVersion apiVersion(short apiKey, short minVersion, short maxVersion) {
        return new ApiVersionsResponseData.ApiVersion().setApiKey(apiKey).setMinVersion(minVersion).setMaxVersion(maxVersion);
    }

    private static ApiVersionsResponseData apiVersionsResponse(short errorCode, ApiVersionsResponseData.ApiVersion... versions) {
        ApiVersionsResponseData.ApiVersionCollection collection = new ApiVersionsResponseData.ApiVersionCollection(versions.length);
        collection.addAll(List.of(versions));
        return new ApiVersionsResponseData().setErrorCode(errorCode).setApiKeys(collection);
    }

    private SubjectRoutingHandler newHandler() {
        RouteSelectorContext selectorContext = new RouteSelectorContextImpl(Set.of("team-a", "team-b"));
        return new SubjectRoutingHandler(selector, selectorContext, "vc1", "subj-router");
    }

    @Test
    void staticRoutesIsEmpty() {
        // Given
        SubjectRoutingHandler handler = newHandler();

        // When
        var staticRoutes = handler.staticRoutes();

        // Then
        assertThat(staticRoutes).isEmpty();
    }

    @Test
    void mappedSubjectForwardsToSelectedRoute() {
        // Given
        Subject subject = new Subject(Set.of(new User("CN=alice")));
        when(ctx.authenticatedSubject()).thenReturn(subject);
        when(selector.selectRoute(eq(subject), any())).thenReturn(CompletableFuture.completedFuture(Optional.of("team-a")));
        VirtualNode node = mock(VirtualNode.class);
        when(ctx.anyNode("team-a")).thenReturn(node);
        MetadataResponseData upstreamResponse = new MetadataResponseData();
        when(ctx.sendRequest(node, HEADER, REQUEST)).thenReturn(CompletableFuture.completedFuture(upstreamResponse));
        RouterResponse routerResponse = mock(RouterResponse.class);
        when(ctx.respondWith((ApiMessage) upstreamResponse)).thenReturn(closeableStage(routerResponse));

        // When
        RouterResponse result = newHandler().onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(routerResponse);
        verify(ctx).sendRequest(node, HEADER, REQUEST);
    }

    @Test
    void anonymousNonApiVersionsRequestRejectedAndConnectionClosed() {
        // Given
        when(ctx.authenticatedSubject()).thenReturn(Subject.anonymous());
        RouterResponse errorResponse = mock(RouterResponse.class);
        when(ctx.respondWithError(HEADER, REQUEST, Errors.SASL_AUTHENTICATION_FAILED)).thenReturn(closeableStage(errorResponse));

        // When
        RouterResponse result = newHandler().onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(errorResponse);
        verify(ctx).respondWithError(HEADER, REQUEST, Errors.SASL_AUTHENTICATION_FAILED);
    }

    @Test
    void unmappedSubjectRejected() {
        // Given
        Subject subject = new Subject(Set.of(new User("CN=eve")));
        when(ctx.authenticatedSubject()).thenReturn(subject);
        when(selector.selectRoute(eq(subject), any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        RouterResponse errorResponse = mock(RouterResponse.class);
        when(ctx.respondWithError(HEADER, REQUEST, Errors.SASL_AUTHENTICATION_FAILED)).thenReturn(closeableStage(errorResponse));

        // When
        RouterResponse result = newHandler().onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(errorResponse);
    }

    @Test
    void selectorErrorRejected() {
        // Given
        Subject subject = new Subject(Set.of(new User("CN=alice")));
        when(ctx.authenticatedSubject()).thenReturn(subject);
        when(selector.selectRoute(eq(subject), any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));
        RouterResponse errorResponse = mock(RouterResponse.class);
        when(ctx.respondWithError(HEADER, REQUEST, Errors.SASL_AUTHENTICATION_FAILED)).thenReturn(closeableStage(errorResponse));

        // When
        RouterResponse result = newHandler().onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(errorResponse);
    }

    @Test
    void anonymousApiVersionsFansOutAndIntersectsAcrossRoutes() {
        // Given
        when(ctx.authenticatedSubject()).thenReturn(Subject.anonymous());
        VirtualNode nodeA = mock(VirtualNode.class);
        VirtualNode nodeB = mock(VirtualNode.class);
        when(ctx.anyNode("team-a")).thenReturn(nodeA);
        when(ctx.anyNode("team-b")).thenReturn(nodeB);
        RequestHeaderData headerCopy = new RequestHeaderData();
        MetadataRequestData requestCopy = new MetadataRequestData();
        when(ctx.sendRequest(eq(nodeA), any(RequestHeaderData.class), any(ApiMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        apiVersionsResponse((short) 0, apiVersion(ApiKeys.METADATA.id, (short) 0, (short) 9), apiVersion(ApiKeys.PRODUCE.id, (short) 0, (short) 8))));
        when(ctx.sendRequest(eq(nodeB), any(RequestHeaderData.class), any(ApiMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        apiVersionsResponse((short) 0, apiVersion(ApiKeys.METADATA.id, (short) 2, (short) 12), apiVersion(ApiKeys.FETCH.id, (short) 0, (short) 11))));
        RouterResponse routerResponse = mock(RouterResponse.class);
        when(ctx.respondWith(any(ApiVersionsResponseData.class))).thenReturn(closeableStage(routerResponse));

        // When
        RouterResponse result = newHandler().onRequest(ApiKeys.API_VERSIONS, (short) 3, headerCopy, requestCopy, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(routerResponse);
        ArgumentCaptor<ApiVersionsResponseData> captor = ArgumentCaptor.forClass(ApiVersionsResponseData.class);
        verify(ctx).respondWith(captor.capture());
        ApiVersionsResponseData intersection = captor.getValue();
        assertThat(intersection.errorCode()).isZero();
        assertThat(intersection.apiKeys()).hasSize(1);
        ApiVersionsResponseData.ApiVersion metadata = intersection.apiKeys().find(apiVersion(ApiKeys.METADATA.id, (short) 0, (short) 0));
        assertThat(metadata.minVersion()).isEqualTo((short) 2);
        assertThat(metadata.maxVersion()).isEqualTo((short) 9);
    }

    @Test
    void anonymousApiVersionsRejectedWhenAnyRouteErrors() {
        // Given
        when(ctx.authenticatedSubject()).thenReturn(Subject.anonymous());
        VirtualNode nodeA = mock(VirtualNode.class);
        VirtualNode nodeB = mock(VirtualNode.class);
        when(ctx.anyNode("team-a")).thenReturn(nodeA);
        when(ctx.anyNode("team-b")).thenReturn(nodeB);
        when(ctx.sendRequest(eq(nodeA), any(RequestHeaderData.class), any(ApiMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(apiVersionsResponse((short) 0, apiVersion(ApiKeys.METADATA.id, (short) 0, (short) 9))));
        when(ctx.sendRequest(eq(nodeB), any(RequestHeaderData.class), any(ApiMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(apiVersionsResponse(Errors.UNKNOWN_SERVER_ERROR.code())));
        RouterResponse errorResponse = mock(RouterResponse.class);
        when(ctx.respondWithError(HEADER, REQUEST, Errors.SASL_AUTHENTICATION_FAILED)).thenReturn(closeableStage(errorResponse));

        // When
        RouterResponse result = newHandler().onRequest(ApiKeys.API_VERSIONS, (short) 3, HEADER, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(errorResponse);
    }

    @Test
    void repeatedRequestsForSameRouteAllForward() {
        // Given
        Subject subject = new Subject(Set.of(new User("CN=alice")));
        when(ctx.authenticatedSubject()).thenReturn(subject);
        when(selector.selectRoute(eq(subject), any())).thenReturn(CompletableFuture.completedFuture(Optional.of("team-a")));
        VirtualNode node = mock(VirtualNode.class);
        when(ctx.anyNode("team-a")).thenReturn(node);
        MetadataResponseData upstreamResponse = new MetadataResponseData();
        when(ctx.sendRequest(node, HEADER, REQUEST)).thenReturn(CompletableFuture.completedFuture(upstreamResponse));
        RouterResponse routerResponse = mock(RouterResponse.class);
        when(ctx.respondWith((ApiMessage) upstreamResponse)).thenReturn(closeableStage(routerResponse));
        SubjectRoutingHandler handler = newHandler();
        handler.onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx).toCompletableFuture().join();

        // When
        RouterResponse result = handler.onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(routerResponse);
        verify(ctx, times(2)).sendRequest(node, HEADER, REQUEST);
    }

    @Test
    void routeChangeMidConnectionRejectedAndConnectionClosed() {
        // Given
        Subject subject = new Subject(Set.of(new User("CN=alice")));
        when(ctx.authenticatedSubject()).thenReturn(subject);
        VirtualNode nodeA = mock(VirtualNode.class);
        when(ctx.anyNode("team-a")).thenReturn(nodeA);
        MetadataResponseData upstreamResponse = new MetadataResponseData();
        when(ctx.sendRequest(nodeA, HEADER, REQUEST)).thenReturn(CompletableFuture.completedFuture(upstreamResponse));
        RouterResponse forwardedResponse = mock(RouterResponse.class);
        when(ctx.respondWith((ApiMessage) upstreamResponse)).thenReturn(closeableStage(forwardedResponse));
        RouterResponse errorResponse = mock(RouterResponse.class);
        when(ctx.respondWithError(HEADER, REQUEST, Errors.SASL_AUTHENTICATION_FAILED)).thenReturn(closeableStage(errorResponse));
        SubjectRoutingHandler handler = newHandler();
        when(selector.selectRoute(eq(subject), any())).thenReturn(CompletableFuture.completedFuture(Optional.of("team-a")));
        handler.onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx).toCompletableFuture().join();
        when(selector.selectRoute(eq(subject), any())).thenReturn(CompletableFuture.completedFuture(Optional.of("team-b")));

        // When
        RouterResponse result = handler.onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx)
                .toCompletableFuture().join();

        // Then
        assertThat(result).isSameAs(errorResponse);
    }

    @Test
    void rejectionIncrementsRejectedCounterWithBoundedReasonTag() {
        // Given
        when(ctx.authenticatedSubject()).thenReturn(Subject.anonymous());
        when(ctx.respondWithError(HEADER, REQUEST, Errors.SASL_AUTHENTICATION_FAILED)).thenReturn(closeableStage(mock(RouterResponse.class)));

        // When
        newHandler().onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx).toCompletableFuture().join();

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_subject_router_rejected_total")
                .tags("virtual_cluster", "vc1", "router", "subj-router", "reason", "anonymous")
                .counter().count()).isEqualTo(1);
    }
}
