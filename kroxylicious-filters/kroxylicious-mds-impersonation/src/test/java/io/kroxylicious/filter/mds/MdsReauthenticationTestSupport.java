/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.internal.filter.RequestFilterResultBuilderImpl;
import io.kroxylicious.testing.filter.context.MockFilterContext;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@SuppressWarnings("removal")
class MdsReauthenticationTestSupport {
    static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    final Clock clock = mock(Clock.class);
    final FilterDispatchExecutor executor = mock(FilterDispatchExecutor.class);
    final List<String> users = new ArrayList<>();
    final Queue<CompletionStage<MdsToken>> tokens = new ArrayDeque<>();
    final Queue<CompletionStage<? extends ApiMessage>> replies = new ArrayDeque<>();
    final List<ApiMessage> sent = new ArrayList<>();
    final RequestHeaderData header = new RequestHeaderData().setRequestApiKey(ApiKeys.API_VERSIONS.id).setRequestApiVersion((short) 3);
    final ApiVersionsRequestData request = new ApiVersionsRequestData();
    final MockFilterContext context;
    final MdsImpersonationFilter filter;

    MdsReauthenticationTestSupport() {
        when(clock.instant()).thenReturn(START);
        when(executor.completeOnFilterDispatchThread(any())).thenAnswer(invocation -> invocation.getArgument(0));
        var certificate = mock(X509Certificate.class);
        context = spy(MockFilterContext.builder(header, request).withClientTlsContext(certificate, certificate)
                .withAuthenticatedSubject(new Subject(new User("alice"))).build());
        when(context.requestFilterResultBuilder()).thenAnswer(ignored -> new RequestFilterResultBuilderImpl());
        doAnswer(invocation -> {
            sent.add(invocation.getArgument(1));
            if (((ApiMessage) invocation.getArgument(1)).apiKey() == ApiKeys.API_VERSIONS.id) {
                return CompletableFuture.completedStage(TestSaslVersions.supported());
            }
            return replies.remove();
        }).when(context).sendRequest(any(), any());
        filter = new MdsImpersonationFilter(user -> {
            users.add(user);
            return tokens.remove();
        }, Duration.ofSeconds(5), executor, clock);
    }

    void token(String value, long expiresAfterSeconds) {
        tokens.add(CompletableFuture.completedStage(new MdsToken(value, START.plusSeconds(expiresAfterSeconds))));
    }

    void success(long lifetimeMs) {
        replies.add(CompletableFuture.completedStage(new SaslHandshakeResponseData().setMechanisms(List.of("OAUTHBEARER"))));
        replies.add(CompletableFuture.completedStage(new SaslAuthenticateResponseData().setSessionLifetimeMs(lifetimeMs)));
    }

    CompletableFuture<RequestFilterResult> request() {
        return filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture();
    }

    void time(long seconds) {
        when(clock.instant()).thenReturn(START.plusSeconds(seconds));
    }
}
