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
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.internal.filter.RequestFilterResultBuilderImpl;
import io.kroxylicious.testing.filter.context.MockFilterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@SuppressWarnings("removal")
class MdsIdentityTest {
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private final RequestHeaderData header = new RequestHeaderData().setRequestApiKey(ApiKeys.API_VERSIONS.id).setRequestApiVersion((short) 3);
    private final ApiVersionsRequestData request = new ApiVersionsRequestData();

    private MdsImpersonationFilter filter(Function<String, CompletionStage<MdsToken>> tokens) {
        var executor = mock(FilterDispatchExecutor.class);
        when(executor.completeOnFilterDispatchThread(any())).thenAnswer(invocation -> invocation.getArgument(0));
        return new MdsImpersonationFilter(tokens, Duration.ofSeconds(5), executor, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private MockFilterContext context(Subject subject) {
        var certificate = mock(X509Certificate.class);
        var context = spy(MockFilterContext.builder(header, request).withClientTlsContext(certificate, certificate)
                .withAuthenticatedSubject(subject)
                .withSendRequestResponseEnqueued(TestSaslVersions.supported())
                .withSendRequestResponseEnqueued(new SaslHandshakeResponseData().setMechanisms(List.of("OAUTHBEARER")))
                .withSendRequestResponseEnqueued(new SaslAuthenticateResponseData()).build());
        when(context.requestFilterResultBuilder()).thenAnswer(ignored -> new RequestFilterResultBuilderImpl());
        return context;
    }

    @ParameterizedTest
    @ValueSource(strings = { "new-client", "service-account@example.test" })
    void delegatesTheMappedCertificateUserToMds(String user) {
        // Given
        var requestedUsers = new ArrayList<String>();
        var filter = filter(principal -> {
            requestedUsers.add(principal);
            return CompletableFuture.completedStage(new MdsToken("token", NOW.plusSeconds(60)));
        });
        var context = context(new Subject(new User(user)));

        // When
        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture().join();

        // Then
        assertThat(requestedUsers).containsExactly(user);
        assertThat(result.closeConnection()).isFalse();
        assertThat(result.message()).isSameAs(request);
        assertThat(context.sendRequestInvocations()).hasSize(3);
    }

    @Test
    void refusesAnAnonymousMapping() {
        // Given
        var filter = filter(user -> {
            throw new AssertionError("MDS must not be called");
        });
        var context = context(Subject.anonymous());

        // When
        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(result.message()).isNull();
        assertThat(context.sendRequestInvocations()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " ", "bad\nuser" })
    void refusesAnInvalidMappedName(String user) {
        // Given
        var filter = filter(principal -> {
            throw new AssertionError("MDS must not be called");
        });
        var context = context(new Subject(new User(user)));

        // When
        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(result.message()).isNull();
        assertThat(context.sendRequestInvocations()).isEmpty();
    }
}
