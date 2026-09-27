/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings({ "java:S5738", "removal" })
class SubjectRoutingHandlerTest {

    private static final String SESSION_ID = "session-1";
    private static final RequestHeaderData HEADER = new RequestHeaderData();
    private static final MetadataRequestData REQUEST = new MetadataRequestData();

    @Mock
    private RouterContext ctx;

    @Mock
    private RouteSelector<Object> selector;

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

    private SubjectRoutingHandler newHandler() {
        RouteSelectorContext selectorContext = new RouteSelectorContextImpl(Set.of("team-a", "team-b"));
        return new SubjectRoutingHandler(selector, selectorContext, "vc1", "subj-router");
    }

    @Test
    void staticRoutesIsEmpty() {
        assertThat(newHandler().staticRoutes()).isEmpty();
    }

    @Test
    void mappedSubjectForwardsToSelectedRoute() {
        Subject subject = new Subject(Set.of(new User("CN=alice")));
        when(ctx.authenticatedSubject()).thenReturn(subject);
        when(selector.selectRoute(eq(subject), any())).thenReturn(CompletableFuture.completedFuture(Optional.of("team-a")));
        VirtualNode node = mock(VirtualNode.class);
        when(ctx.anyNode("team-a")).thenReturn(node);
        MetadataResponseData upstreamResponse = new MetadataResponseData();
        when(ctx.sendRequest(node, HEADER, REQUEST)).thenReturn(CompletableFuture.completedFuture(upstreamResponse));
        RouterResponse routerResponse = mock(RouterResponse.class);
        when(ctx.respondWith((ApiMessage) upstreamResponse)).thenReturn(closeableStage(routerResponse));

        RouterResponse result = newHandler().onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx)
                .toCompletableFuture().join();

        assertThat(result).isSameAs(routerResponse);
        verify(ctx).sendRequest(node, HEADER, REQUEST);
    }

    @Test
    void anonymousSubjectRejectedAndConnectionClosed() {
        when(ctx.authenticatedSubject()).thenReturn(Subject.anonymous());
        RouterResponse errorResponse = mock(RouterResponse.class);
        when(ctx.respondWithError(HEADER, REQUEST, Errors.SASL_AUTHENTICATION_FAILED)).thenReturn(closeableStage(errorResponse));

        RouterResponse result = newHandler().onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx)
                .toCompletableFuture().join();

        assertThat(result).isSameAs(errorResponse);
        verify(ctx).respondWithError(HEADER, REQUEST, Errors.SASL_AUTHENTICATION_FAILED);
    }

    @Test
    void unmappedSubjectRejected() {
        Subject subject = new Subject(Set.of(new User("CN=eve")));
        when(ctx.authenticatedSubject()).thenReturn(subject);
        when(selector.selectRoute(eq(subject), any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        RouterResponse errorResponse = mock(RouterResponse.class);
        when(ctx.respondWithError(HEADER, REQUEST, Errors.SASL_AUTHENTICATION_FAILED)).thenReturn(closeableStage(errorResponse));

        RouterResponse result = newHandler().onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx)
                .toCompletableFuture().join();

        assertThat(result).isSameAs(errorResponse);
    }

    @Test
    void selectorErrorRejected() {
        Subject subject = new Subject(Set.of(new User("CN=alice")));
        when(ctx.authenticatedSubject()).thenReturn(subject);
        when(selector.selectRoute(eq(subject), any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));
        RouterResponse errorResponse = mock(RouterResponse.class);
        when(ctx.respondWithError(HEADER, REQUEST, Errors.SASL_AUTHENTICATION_FAILED)).thenReturn(closeableStage(errorResponse));

        RouterResponse result = newHandler().onRequest(ApiKeys.METADATA, (short) 0, HEADER, REQUEST, ctx)
                .toCompletableFuture().join();

        assertThat(result).isSameAs(errorResponse);
    }
}
