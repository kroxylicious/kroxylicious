/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.Errors;
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

@SuppressWarnings("removal") // FilterContext still exposes the transitional Subject API.
class MdsImpersonationFilterTest {
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private final Clock clock = mock(Clock.class);
    private final FilterDispatchExecutor executor = mock(FilterDispatchExecutor.class);
    private final RequestHeaderData header = new RequestHeaderData().setRequestApiKey(ApiKeys.API_VERSIONS.id).setRequestApiVersion((short) 3);
    private final ApiVersionsRequestData request = new ApiVersionsRequestData();

    private MdsImpersonationFilter filter(Function<String, CompletionStage<MdsToken>> tokens) {
        when(clock.instant()).thenReturn(NOW);
        when(executor.completeOnFilterDispatchThread(any())).thenAnswer(invocation -> invocation.getArgument(0));
        return new MdsImpersonationFilter(tokens, Duration.ofSeconds(5), executor, clock);
    }

    private MockFilterContext.MockFilterContextBuilder context(String user) {
        var cert = mock(X509Certificate.class);
        return MockFilterContext.builder(header, request).withClientTlsContext(cert, cert)
                .withAuthenticatedSubject(new Subject(new User(user)))
                .withSendRequestResponseEnqueued(TestSaslVersions.supported())
                .withSendRequestResponseEnqueued(new SaslHandshakeResponseData().setMechanisms(List.of("OAUTHBEARER")));
    }

    private MockFilterContext runtimeResults(MockFilterContext context) {
        // The shared mock retains the incoming message on close; the runtime starts with an empty result.
        var spied = spy(context);
        when(spied.requestFilterResultBuilder()).thenAnswer(ignored -> new RequestFilterResultBuilderImpl());
        return spied;
    }

    @Test
    void holdsClientTrafficUntilTokenAndUpstreamAuthenticationComplete() {
        // Given
        var pending = new CompletableFuture<MdsToken>();
        var filter = filter(user -> pending);
        var context = context("alice").withSendRequestResponseEnqueued(new SaslAuthenticateResponseData()).build();
        context = runtimeResults(context);
        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture();
        boolean wasPending = !result.isDone();
        boolean sentBeforeToken = !context.sendRequestInvocations().isEmpty();

        // When
        pending.complete(new MdsToken("alice-token", NOW.plusSeconds(60)));

        // Then
        assertThat(wasPending).isTrue();
        assertThat(sentBeforeToken).isFalse();
        assertThat(result.join().message()).isSameAs(request);
        assertThat(result.join().closeConnection()).isFalse();
        assertThat(context.sendRequestInvocations()).hasSize(3);
        assertThat(context.sendRequestInvocations().get(0).request().apiKey()).isEqualTo(ApiKeys.API_VERSIONS.id);
        var auth = (SaslAuthenticateRequestData) context.sendRequestInvocations().get(2).request();
        assertThat(new String(auth.authBytes(), StandardCharsets.UTF_8)).isEqualTo("n,,\u0001auth=Bearer alice-token\u0001\u0001");
    }

    @Test
    void keepsUsersAndReconnectsIsolated() {
        // Given
        var requestedUsers = new ArrayList<String>();
        Function<String, CompletionStage<MdsToken>> tokens = user -> {
            requestedUsers.add(user);
            return CompletableFuture.completedStage(new MdsToken(user + "-token", NOW.plusSeconds(60)));
        };

        // When
        for (String user : List.of("alice", "bob", "alice")) {
            var filter = filter(tokens);
            var context = context(user).withSendRequestResponseEnqueued(new SaslAuthenticateResponseData()).build();
            context = runtimeResults(context);
            filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture().join();
            filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture().join();
        }

        // Then
        assertThat(requestedUsers).containsExactly("alice", "bob", "alice");
    }

    @Test
    void refusesIdentityWithoutClientCertificate() {
        // Given
        var filter = filter(user -> {
            throw new AssertionError("MDS must not be called");
        });
        var context = MockFilterContext.builder(header, request).withAuthenticatedSubject(new Subject(new User("alice"))).build();
        context = runtimeResults(context);

        // When
        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(result.message()).isNull();
        assertThat(context.sendRequestInvocations()).isEmpty();
    }

    @Test
    void closesOnMdsFailureWithoutForwarding() {
        // Given
        var filter = filter(user -> CompletableFuture.failedStage(new IllegalStateException("MDS unavailable")));
        var context = context("alice").build();
        context = runtimeResults(context);

        // When
        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(result.message()).isNull();
        assertThat(context.sendRequestInvocations()).isEmpty();
    }

    @Test
    void closesOnBrokerTokenRejection() {
        // Given
        var filter = filter(user -> CompletableFuture.completedStage(new MdsToken("token", NOW.plusSeconds(60))));
        var context = context("alice").withSendRequestResponseEnqueued(
                new SaslAuthenticateResponseData().setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())).build();
        context = runtimeResults(context);

        // When
        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(result.message()).isNull();
    }

    @Test
    void honorsBrokerSessionDeadlineBeforeJwtExpiry() {
        // Given
        var filter = filter(user -> CompletableFuture.completedStage(new MdsToken("token", NOW.plusSeconds(3600))));
        var context = context("alice").withSendRequestResponseEnqueued(new SaslAuthenticateResponseData().setSessionLifetimeMs(30000))
                .withSendRequestResponseEnqueued(new SaslHandshakeResponseData().setMechanisms(List.of("OAUTHBEARER")))
                .withSendRequestResponseEnqueued(new SaslAuthenticateResponseData().setSessionLifetimeMs(30000)).build();
        context = runtimeResults(context);
        filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture().join();
        when(clock.instant()).thenReturn(NOW.plusSeconds(25));

        // When
        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture().join();

        // Then
        assertThat(result.closeConnection()).isFalse();
        assertThat(result.message()).isSameAs(request);
        assertThat(context.sendRequestInvocations()).hasSize(5);
    }

    @Test
    void refusesAlreadyExpiringToken() {
        // Given
        var filter = filter(user -> CompletableFuture.completedStage(new MdsToken("token", NOW.plusSeconds(5))));
        var context = context("alice").build();
        context = runtimeResults(context);

        // When
        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 3, header, request, context).toCompletableFuture().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(context.sendRequestInvocations()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = { "SASL_HANDSHAKE", "SASL_AUTHENTICATE" })
    void rejectsDownstreamSasl(ApiKeys key) {
        // Given
        var filter = filter(user -> {
            throw new AssertionError("MDS must not be called");
        });
        var context = context("alice").build();
        context = runtimeResults(context);

        // When
        var result = filter.onRequest(key, (short) 1, header, request, context).toCompletableFuture().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(context.sendRequestInvocations()).isEmpty();
    }
}
