/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServerProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SaslTerminationFilterTest {

    private static final Instant FIXED_INSTANT = Instant.parse("2026-01-01T00:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_INSTANT, ZoneOffset.UTC);
    private static final String TEST_VIRTUAL_CLUSTER = "test-cluster";

    private SimpleMeterRegistry meterRegistry;

    @BeforeAll
    static void registerProvider() {
        OAuthBearerSaslServerProvider.initialize();
    }

    @BeforeEach
    void setUpMetrics() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
    }

    @AfterEach
    void tearDownMetrics() {
        if (meterRegistry != null) {
            meterRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
            Metrics.globalRegistry.remove(meterRegistry);
        }
    }

    // --- Handshake flow ---

    @Test
    void shouldRespondWithNoneForSupportedHandshake() throws Exception {
        // Given
        var filter = createFilter();
        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var filterContext = mockShortCircuitFilterContext(captor);

        // When
        filter.onRequest(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(),
                new RequestHeaderData(),
                new SaslHandshakeRequestData().setMechanism("OAUTHBEARER"),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslHandshakeResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.NONE.code());
    }

    @Test
    void shouldCloseConnectionForUnsupportedHandshake() throws Exception {
        // Given
        var filter = createFilter();
        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var terminal = mock(TerminalStage.class);
        var filterContext = mockShortCircuitFilterContextWithCloseTracking(captor, closeOrTerminal, terminal);

        // When
        filter.onRequest(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(),
                new RequestHeaderData(),
                new SaslHandshakeRequestData().setMechanism("PLAIN"),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslHandshakeResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.UNSUPPORTED_SASL_MECHANISM.code());
        assertThat(response.mechanisms()).contains("OAUTHBEARER");
        verify(closeOrTerminal).withCloseConnection();
    }

    @Test
    void shouldRejectHandshakeInRequiringAuthenticateState() throws Exception {
        // Given
        var filter = createFilter();
        var handler = mock(MechanismStateMachine.class);
        setFilterState(filter, State.start().nextState(handler, 0L));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var filterContext = mockShortCircuitFilterContext(captor);

        // When
        filter.onRequest(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(),
                new RequestHeaderData(),
                new SaslHandshakeRequestData().setMechanism("OAUTHBEARER"),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslHandshakeResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.ILLEGAL_SASL_STATE.code());
    }

    @Test
    void shouldRejectHandshakeInFailedState() throws Exception {
        // Given
        var filter = createFilter();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler, 0L);
        setFilterState(filter, authenticating.nextStateFailure("previous failure"));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var filterContext = mockShortCircuitFilterContext(captor);

        // When
        filter.onRequest(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(),
                new RequestHeaderData(),
                new SaslHandshakeRequestData().setMechanism("OAUTHBEARER"),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslHandshakeResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.ILLEGAL_SASL_STATE.code());
    }

    @Test
    void shouldAllowReauthenticationHandshake() throws Exception {
        // Given
        var filter = createFilter();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler, 0L);
        setFilterState(filter, authenticating.nextStateSuccess("alice", "OAUTHBEARER", null));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var filterContext = mockShortCircuitFilterContext(captor);

        // When
        filter.onRequest(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(),
                new RequestHeaderData(),
                new SaslHandshakeRequestData().setMechanism("OAUTHBEARER"),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslHandshakeResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.NONE.code());
    }

    // --- Authenticate flow ---

    @Test
    void shouldRejectAuthenticateInRequiringHandshakeState() throws Exception {
        // Given
        var filter = createFilter();
        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var filterContext = mockShortCircuitFilterContext(captor);

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                new RequestHeaderData(),
                new SaslAuthenticateRequestData().setAuthBytes(new byte[0]),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslAuthenticateResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.ILLEGAL_SASL_STATE.code());
        assertThat(response.errorMessage()).isEqualTo("Authentication not in progress");
    }

    @Test
    void shouldRejectAuthenticateInAuthenticatedState() throws Exception {
        // Given
        var filter = createFilter();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler, 0L);
        setFilterState(filter, authenticating.nextStateSuccess("alice", "OAUTHBEARER", null));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var filterContext = mockShortCircuitFilterContext(captor);

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                new RequestHeaderData(),
                new SaslAuthenticateRequestData().setAuthBytes(new byte[0]),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslAuthenticateResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.ILLEGAL_SASL_STATE.code());
        assertThat(response.errorMessage()).isEqualTo("Authentication not in progress");
    }

    @Test
    void shouldReturnChallengeBytes() throws Exception {
        // Given
        byte[] challengeBytes = { 10, 20, 30 };
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.challenge(challengeBytes)));

        var filter = createFilterWithZeroDelay();
        setFilterState(filter, State.start().nextState(handler, System.nanoTime()));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var filterContext = mockShortCircuitFilterContext(captor);

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                new RequestHeaderData(),
                new SaslAuthenticateRequestData().setAuthBytes(new byte[]{ 1, 2 }),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslAuthenticateResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(response.authBytes()).containsExactly(10, 20, 30);
    }

    @Test
    void shouldReturnSuccessResponse() throws Exception {
        // Given
        byte[] responseBytes = { 40, 50 };
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.success(responseBytes, "alice", 3600000)));

        var filter = createFilterWithZeroDelay();
        setFilterState(filter, State.start().nextState(handler, System.nanoTime()));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var filterContext = mockShortCircuitFilterContextForSuccess(captor);

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                new RequestHeaderData(),
                new SaslAuthenticateRequestData().setAuthBytes(new byte[]{ 1, 2 }),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslAuthenticateResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(response.authBytes()).containsExactly(40, 50);
        assertThat(response.sessionLifetimeMs()).isEqualTo(3600000);
        assertThat(meterRegistry.find(SaslTerminationFilter.AUTH_DURATION_METRIC)
                .tags(List.of(Tag.of("mechanism", "OAUTHBEARER"), Tag.of(SaslTerminationFilter.VIRTUAL_CLUSTER_TAG, TEST_VIRTUAL_CLUSTER)))
                .timer()).isNotNull();
    }

    @Test
    void shouldCallClientSaslAuthenticationSuccessOnSuccess() throws Exception {
        // Given
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.success(new byte[0], "alice", 0)));

        var filter = createFilterWithZeroDelay();
        setFilterState(filter, State.start().nextState(handler, System.nanoTime()));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var filterContext = mockShortCircuitFilterContextForSuccess(captor);

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                new RequestHeaderData(),
                new SaslAuthenticateRequestData().setAuthBytes(new byte[0]),
                filterContext).toCompletableFuture().get();

        // Then
        verify(filterContext).clientSaslAuthenticationSuccess(eq("OAUTHBEARER"), any(Subject.class));
    }

    @Test
    void shouldReturnFailureAndCloseConnection() throws Exception {
        // Given
        var exception = new javax.security.sasl.SaslException("bad credentials");
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.failure(new byte[0], exception)));

        var filter = createFilterWithZeroDelay();
        setFilterState(filter, State.start().nextState(handler, System.nanoTime()));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var terminal = mock(TerminalStage.class);
        var filterContext = mockShortCircuitFilterContextWithCloseTracking(captor, closeOrTerminal, terminal);

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                new RequestHeaderData(),
                new SaslAuthenticateRequestData().setAuthBytes(new byte[0]),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslAuthenticateResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
        assertThat(response.errorMessage()).isEqualTo("Authentication failed");
        verify(closeOrTerminal).withCloseConnection();
        verify(filterContext).clientSaslAuthenticationFailure(eq("OAUTHBEARER"), isNull(), eq(exception));
        assertThat(meterRegistry.find(SaslTerminationFilter.AUTH_DURATION_METRIC)
                .tags(List.of(Tag.of("mechanism", "OAUTHBEARER"), Tag.of(SaslTerminationFilter.VIRTUAL_CLUSTER_TAG, TEST_VIRTUAL_CLUSTER)))
                .timer()).isNotNull();
    }

    @Test
    void shouldHandleEvaluateRoundException() throws Exception {
        // Given
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        var failedFuture = new CompletableFuture<RoundResult>();
        failedFuture.completeExceptionally(new RuntimeException("credential store unavailable"));
        when(handler.evaluateRound(any())).thenReturn(failedFuture);

        var filter = createFilterWithZeroDelay();
        setFilterState(filter, State.start().nextState(handler, System.nanoTime()));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var terminal = mock(TerminalStage.class);
        var filterContext = mockShortCircuitFilterContextWithCloseTracking(captor, closeOrTerminal, terminal);

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                new RequestHeaderData(),
                new SaslAuthenticateRequestData().setAuthBytes(new byte[0]),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslAuthenticateResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
        verify(closeOrTerminal).withCloseConnection();
    }

    @Test
    void shouldDisposeStateMachineOnSuccess() throws Exception {
        // Given
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.success(new byte[0], "alice", 0)));

        var filter = createFilterWithZeroDelay();
        setFilterState(filter, State.start().nextState(handler, System.nanoTime()));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var filterContext = mockShortCircuitFilterContextForSuccess(captor);

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                new RequestHeaderData(),
                new SaslAuthenticateRequestData().setAuthBytes(new byte[0]),
                filterContext).toCompletableFuture().get();

        // Then
        verify(handler).dispose();
    }

    @Test
    void shouldDisposeStateMachineOnFailure() throws Exception {
        // Given
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.failure(new byte[0], new Exception("fail"))));

        var filter = createFilterWithZeroDelay();
        setFilterState(filter, State.start().nextState(handler, System.nanoTime()));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var terminal = mock(TerminalStage.class);
        var filterContext = mockShortCircuitFilterContextWithCloseTracking(captor, closeOrTerminal, terminal);

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                new RequestHeaderData(),
                new SaslAuthenticateRequestData().setAuthBytes(new byte[0]),
                filterContext).toCompletableFuture().get();

        // Then
        verify(handler).dispose();
    }

    // --- Reauthentication consistency ---

    @SuppressWarnings("unchecked")
    @Test
    void shouldRejectReauthenticationWithDifferentMechanism() throws Exception {
        // Given
        var filter = createFilterWithZeroDelay();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler, 0L);
        setFilterState(filter, authenticating.nextStateSuccess("alice", "OAUTHBEARER", null));

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var terminal = mock(TerminalStage.class);
        var filterContext = mockShortCircuitFilterContextWithCloseTracking(captor, closeOrTerminal, terminal);

        // When
        filter.onRequest(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(),
                new RequestHeaderData(),
                new SaslHandshakeRequestData().setMechanism("SCRAM-SHA-256"),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslHandshakeResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.ILLEGAL_SASL_STATE.code());
        verify(closeOrTerminal).withCloseConnection();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldRejectReauthenticationWithDifferentAuthorizationId() throws Exception {
        // Given
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.success(new byte[0], "bob", 0)));

        var filter = createFilterWithZeroDelay();
        var initialHandler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(initialHandler, 0L);
        var authenticated = authenticating.nextStateSuccess("alice", "OAUTHBEARER", null);
        var reauthenticating = authenticated.nextStateReauthenticate(handler, System.nanoTime());
        setFilterState(filter, reauthenticating);

        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var terminal = mock(TerminalStage.class);
        var filterContext = mockShortCircuitFilterContextWithCloseTracking(captor, closeOrTerminal, terminal);

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                new RequestHeaderData(),
                new SaslAuthenticateRequestData().setAuthBytes(new byte[0]),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslAuthenticateResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
        verify(closeOrTerminal).withCloseConnection();
    }

    // --- Default request / security barrier ---

    @Test
    void shouldRejectUnauthenticatedDefaultRequest() throws Exception {
        // Given
        var filter = createFilter();
        var exceptionCaptor = ArgumentCaptor.forClass(ApiException.class);
        var filterContext = mockErrorFilterContextWithClose(exceptionCaptor);

        // When
        filter.onRequest(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion(),
                new RequestHeaderData(),
                new MetadataRequestData(),
                filterContext).toCompletableFuture().get();

        // Then
        assertThat(exceptionCaptor.getValue()).isInstanceOf(org.apache.kafka.common.errors.SaslAuthenticationException.class);
    }

    @Test
    void shouldForwardAuthenticatedRequestWithNoExpiry() throws Exception {
        // Given
        var filter = createFilter();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler, 0L);
        setFilterState(filter, authenticating.nextStateSuccess("alice", "OAUTHBEARER", null));

        var filterContext = mockForwardingFilterContext();

        // When
        filter.onRequest(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion(),
                new RequestHeaderData(),
                new MetadataRequestData(),
                filterContext).toCompletableFuture().get();

        // Then
        verify(filterContext).forwardRequest(any(), any());
    }

    @Test
    void shouldForwardAuthenticatedRequestWithFutureExpiry() throws Exception {
        // Given
        var filter = createFilterWithClock(FIXED_CLOCK);
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler, 0L);
        Instant futureExpiry = FIXED_INSTANT.plusSeconds(3600);
        setFilterState(filter, authenticating.nextStateSuccess("alice", "OAUTHBEARER", futureExpiry));

        var filterContext = mockForwardingFilterContext();

        // When
        filter.onRequest(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion(),
                new RequestHeaderData(),
                new MetadataRequestData(),
                filterContext).toCompletableFuture().get();

        // Then
        verify(filterContext).forwardRequest(any(), any());
    }

    @Test
    void shouldRejectExpiredSession() throws Exception {
        // Given
        var filter = createFilterWithClock(FIXED_CLOCK);
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler, 0L);
        Instant pastExpiry = FIXED_INSTANT.minusSeconds(60);
        setFilterState(filter, authenticating.nextStateSuccess("alice", "OAUTHBEARER", pastExpiry));

        var exceptionCaptor = ArgumentCaptor.forClass(ApiException.class);
        var filterContext = mockErrorFilterContextWithClose(exceptionCaptor);

        // When
        filter.onRequest(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion(),
                new RequestHeaderData(),
                new MetadataRequestData(),
                filterContext).toCompletableFuture().get();

        // Then
        assertThat(exceptionCaptor.getValue()).isInstanceOf(org.apache.kafka.common.errors.SaslAuthenticationException.class);
        assertThat(meterRegistry.find(SaslTerminationFilter.SESSION_EXPIRED_METRIC)
                .tags(List.of(Tag.of("mechanism", "OAUTHBEARER"), Tag.of(SaslTerminationFilter.VIRTUAL_CLUSTER_TAG, TEST_VIRTUAL_CLUSTER)))
                .counter()).isNotNull()
                .satisfies(counter -> assertThat(counter.count()).isEqualTo(1));
    }

    // --- Filtered API rejection ---

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = {
            "CREATE_DELEGATION_TOKEN", "RENEW_DELEGATION_TOKEN",
            "EXPIRE_DELEGATION_TOKEN", "DESCRIBE_DELEGATION_TOKEN"
    })
    void shouldRejectDelegationTokenApiWithCorrectMessage(ApiKeys apiKey) throws Exception {
        // Given
        var filter = createFilter();
        var exceptionCaptor = ArgumentCaptor.forClass(ApiException.class);
        var filterContext = mockErrorFilterContextWithoutClose(exceptionCaptor);

        // When
        filter.onRequest(apiKey, apiKey.latestVersion(),
                new RequestHeaderData(),
                apiKey.messageType.newRequest(),
                filterContext).toCompletableFuture().get();

        // Then
        assertThat(exceptionCaptor.getValue().getMessage())
                .isEqualTo("Delegation tokens are not supported when SASL is terminated at the proxy");
    }

    // --- API_VERSIONS ---

    @Test
    void shouldForwardApiVersionsRequest() throws Exception {
        // Given
        var filter = createFilter();
        var filterContext = mockForwardingFilterContext();

        // When
        filter.onRequest(ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.latestVersion(),
                new RequestHeaderData(),
                new ApiVersionsRequestData(),
                filterContext).toCompletableFuture().get();

        // Then
        verify(filterContext).forwardRequest(any(), any());
    }

    // --- computeSessionLifetimeMs ---

    @ParameterizedTest
    @CsvSource({
            "0, 0, 0",
            "5000, 0, 5000",
            "0, 3000, 3000",
            "5000, 3000, 3000",
            "3000, 5000, 3000"
    })
    void shouldComputeSessionLifetimeMs(long maxTimeBeforeReauthMs, long handlerLifetimeMs, long expected) {
        // Given
        Duration maxReauth = maxTimeBeforeReauthMs > 0 ? Duration.ofMillis(maxTimeBeforeReauthMs) : null;
        var context = new SaslTermination.SaslTerminationContext(
                null, Set.of("OAUTHBEARER"), List.of(),
                maxReauth, Clock.systemUTC(), Duration.ZERO,
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(mock(ScheduledExecutorService.class), context);

        // When
        long result = filter.computeSessionLifetimeMs(handlerLifetimeMs);

        // Then
        assertThat(result).isEqualTo(expected);
    }

    // --- Helper methods ---

    private static SaslTerminationFilter createFilter() {
        var callbackHandler = new OAuthBearerValidatorCallbackHandler();
        var context = new SaslTermination.SaslTerminationContext(
                callbackHandler,
                Set.of("OAUTHBEARER"), List.of(),
                null, Clock.systemUTC(), Duration.ofMillis(200),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        return new SaslTerminationFilter(mock(ScheduledExecutorService.class), context);
    }

    private static SaslTerminationFilter createFilterWithZeroDelay() {
        var context = new SaslTermination.SaslTerminationContext(
                null, Set.of("OAUTHBEARER"), List.of(),
                null, Clock.systemUTC(), Duration.ZERO,
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        return new SaslTerminationFilter(mock(ScheduledExecutorService.class), context);
    }

    private static SaslTerminationFilter createFilterWithClock(Clock clock) {
        var callbackHandler = new OAuthBearerValidatorCallbackHandler();
        var context = new SaslTermination.SaslTerminationContext(
                callbackHandler,
                Set.of("OAUTHBEARER"), List.of(),
                null, clock, Duration.ofMillis(200),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        return new SaslTerminationFilter(mock(ScheduledExecutorService.class), context);
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockShortCircuitFilterContext(ArgumentCaptor<ApiMessage> captor) {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(filterContext.sessionId()).thenReturn("test-session");
        when(builder.shortCircuitResponse(captor.capture())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockShortCircuitFilterContextForSuccess(ArgumentCaptor<ApiMessage> captor) {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(filterContext.sessionId()).thenReturn("test-session");
        when(filterContext.getVirtualClusterName()).thenReturn(TEST_VIRTUAL_CLUSTER);
        when(filterContext.clientTlsContext()).thenReturn(Optional.empty());
        when(builder.shortCircuitResponse(captor.capture())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockShortCircuitFilterContextWithCloseTracking(
                                                                                ArgumentCaptor<ApiMessage> captor,
                                                                                CloseOrTerminalStage<RequestFilterResult> closeOrTerminal,
                                                                                TerminalStage<RequestFilterResult> terminal) {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(filterContext.sessionId()).thenReturn("test-session");
        when(filterContext.getVirtualClusterName()).thenReturn(TEST_VIRTUAL_CLUSTER);
        when(builder.shortCircuitResponse(captor.capture())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.withCloseConnection()).thenReturn(terminal);
        when(terminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockErrorFilterContextWithClose(ArgumentCaptor<ApiException> exceptionCaptor) {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var terminal = mock(TerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(filterContext.sessionId()).thenReturn("test-session");
        when(filterContext.getVirtualClusterName()).thenReturn(TEST_VIRTUAL_CLUSTER);
        when(builder.errorResponse(any(), any(), exceptionCaptor.capture())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.withCloseConnection()).thenReturn(terminal);
        when(terminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockErrorFilterContextWithoutClose(ArgumentCaptor<ApiException> exceptionCaptor) {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(filterContext.sessionId()).thenReturn("test-session");
        when(builder.errorResponse(any(), any(), exceptionCaptor.capture())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    private static FilterContext mockForwardingFilterContext() {
        var filterContext = mock(FilterContext.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.sessionId()).thenReturn("test-session");
        when(filterContext.forwardRequest(any(), any())).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    private static void setFilterState(SaslTerminationFilter filter, State state) {
        try {
            var field = SaslTerminationFilter.class.getDeclaredField("state");
            field.setAccessible(true);
            field.set(filter, state);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
