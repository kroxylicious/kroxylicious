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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.security.sasl.SaslException;

import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServerProvider;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.DefaultEventLoop;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage;
import io.kroxylicious.proxy.internal.NettyFilterDispatchExecutor;
import io.kroxylicious.scram.credentialstore.ScramCredentialStore;

import edu.umd.cs.findbugs.annotations.Nullable;

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
        setFilterState(filter, State.start().nextState(handler));

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
        var authenticating = State.start().nextState(handler);
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
        var authenticating = State.start().nextState(handler);
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
        var authenticating = State.start().nextState(handler);
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
        setFilterState(filter, State.start().nextState(handler));

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
        Instant sessionExpiry = FIXED_INSTANT.plusMillis(3600000);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.success(responseBytes, "alice", sessionExpiry)));

        var filter = createFilterWithZeroDelayAndClock(FIXED_CLOCK);
        setFilterState(filter, State.start().nextState(handler));

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
                CompletableFuture.completedFuture(RoundResult.success(new byte[0], "alice")));

        var filter = createFilterWithZeroDelay();
        setFilterState(filter, State.start().nextState(handler));

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
        var exception = new SaslException("bad credentials");
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.failure(new byte[0], exception)));

        var filter = createFilterWithZeroDelay();
        setFilterState(filter, State.start().nextState(handler));

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
        setFilterState(filter, State.start().nextState(handler));

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
    void shouldRejectAuthenticationWhenTokenExpiresBeforeResponseIsSent() throws Exception {
        // Given
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        Instant alreadyExpired = FIXED_INSTANT.minusMillis(1);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.success(new byte[0], "alice", alreadyExpired)));

        var filter = createFilterWithZeroDelayAndClock(FIXED_CLOCK);
        setFilterState(filter, State.start().nextState(handler));

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
        verify(filterContext).clientSaslAuthenticationFailure(eq("OAUTHBEARER"), isNull(), any(Exception.class));
    }

    @Test
    void shouldDisposeStateMachineOnSuccess() throws Exception {
        // Given
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.success(new byte[0], "alice")));

        var filter = createFilterWithZeroDelay();
        setFilterState(filter, State.start().nextState(handler));

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
        setFilterState(filter, State.start().nextState(handler));

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

    @SuppressWarnings("java:S2093") // The recommended try-with-resources for `executor` actually results in deadlock; it's closed with the eventLoop anyway
    @Test
    void shouldRecordAuthDurationExcludingFixedDelay() throws Exception {
        // Given
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(RoundResult.success(new byte[0], "alice")));

        Duration fixedDelay = Duration.ofMillis(200);

        var eventLoop = new DefaultEventLoop();
        try {
            var executor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            var context = new SaslTermination.SaslTerminationContext(
                    null, OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES, Map.of(), Map.of(), Set.of("OAUTHBEARER"), List.of(),
                    null, Clock.systemUTC(), fixedDelay,
                    SaslTermination.DEFAULT_SUBJECT_BUILDER);
            var filter = new SaslTerminationFilter(executor, context);
            setFilterState(filter, State.start().nextState(handler));

            var captor = ArgumentCaptor.forClass(ApiMessage.class);
            var filterContext = mockShortCircuitFilterContextForSuccess(captor);

            // When
            filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                    new RequestHeaderData(),
                    new SaslAuthenticateRequestData().setAuthBytes(new byte[0]),
                    filterContext).toCompletableFuture().get();

            // Then
            var timer = meterRegistry.find(SaslTerminationFilter.AUTH_DURATION_METRIC)
                    .tags(List.of(Tag.of("mechanism", "OAUTHBEARER"), Tag.of(SaslTerminationFilter.VIRTUAL_CLUSTER_TAG, TEST_VIRTUAL_CLUSTER)))
                    .timer();
            assertThat(timer).isNotNull();
            assertThat(timer.totalTime(TimeUnit.MILLISECONDS))
                    .isLessThan((double) fixedDelay.toMillis());
        }
        finally {
            eventLoop.shutdownGracefully().sync();
        }
    }

    // --- Reauthentication consistency ---

    @SuppressWarnings("unchecked")
    @Test
    void shouldRejectReauthenticationWithDifferentMechanism() throws Exception {
        // Given
        var filter = createFilterWithZeroDelay();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler);
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
                CompletableFuture.completedFuture(RoundResult.success(new byte[0], "bob")));

        var filter = createFilterWithZeroDelay();
        var initialHandler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(initialHandler);
        var authenticated = authenticating.nextStateSuccess("alice", "OAUTHBEARER", null);
        var reauthenticating = authenticated.nextStateReauthenticate(handler);
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
        var errorsCaptor = ArgumentCaptor.forClass(Errors.class);
        var filterContext = mockErrorFilterContextWithClose(errorsCaptor);

        // When
        filter.onRequest(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion(),
                new RequestHeaderData(),
                new MetadataRequestData(),
                filterContext).toCompletableFuture().get();

        // Then
        assertThat(errorsCaptor.getValue()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED);
    }

    @Test
    void shouldForwardAuthenticatedRequestWithNoExpiry() throws Exception {
        // Given
        var filter = createFilter();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler);
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
        var authenticating = State.start().nextState(handler);
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
        var authenticating = State.start().nextState(handler);
        Instant pastExpiry = FIXED_INSTANT.minusSeconds(60);
        setFilterState(filter, authenticating.nextStateSuccess("alice", "OAUTHBEARER", pastExpiry));

        var errorsCaptor = ArgumentCaptor.forClass(Errors.class);
        var filterContext = mockErrorFilterContextWithClose(errorsCaptor);

        // When
        filter.onRequest(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion(),
                new RequestHeaderData(),
                new MetadataRequestData(),
                filterContext).toCompletableFuture().get();

        // Then
        assertThat(errorsCaptor.getValue()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED);
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
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler);
        setFilterState(filter, authenticating.nextStateSuccess("alice", "OAUTHBEARER", null));
        var errorsCaptor = ArgumentCaptor.forClass(Errors.class);
        var messageCaptor = ArgumentCaptor.forClass(String.class);
        var filterContext = mockErrorFilterContextWithoutClose(errorsCaptor, messageCaptor);

        // When
        filter.onRequest(apiKey, apiKey.latestVersion(),
                new RequestHeaderData(),
                apiKey.messageType.newRequest(),
                filterContext).toCompletableFuture().get();

        // Then
        assertThat(messageCaptor.getValue())
                .isEqualTo(apiKey + " is not supported when SASL is terminated at the proxy");
    }

    @Test
    void shouldHandshakeWithScramSha512() throws Exception {
        // Given
        var filter = createFilterWithScram512();
        var captor = ArgumentCaptor.forClass(ApiMessage.class);
        var filterContext = mockShortCircuitFilterContext(captor);

        // When
        filter.onRequest(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(),
                new RequestHeaderData(),
                new SaslHandshakeRequestData().setMechanism("SCRAM-SHA-512"),
                filterContext).toCompletableFuture().get();

        // Then
        var response = (SaslHandshakeResponseData) captor.getValue();
        assertThat(response.errorCode()).isEqualTo(Errors.NONE.code());
    }

    @Test
    void shouldRejectAlterScramCredentialsWithCorrectMessage() throws Exception {
        // Given
        var filter = createFilterWithScram();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler);
        setFilterState(filter, authenticating.nextStateSuccess("alice", "SCRAM-SHA-256", null));
        var errorsCaptor = ArgumentCaptor.forClass(Errors.class);
        var messageCaptor = ArgumentCaptor.forClass(String.class);
        var filterContext = mockErrorFilterContextWithoutClose(errorsCaptor, messageCaptor);

        // When
        filter.onRequest(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS, ApiKeys.ALTER_USER_SCRAM_CREDENTIALS.latestVersion(),
                new RequestHeaderData(),
                ApiKeys.ALTER_USER_SCRAM_CREDENTIALS.messageType.newRequest(),
                filterContext).toCompletableFuture().get();

        // Then
        assertThat(messageCaptor.getValue())
                .isEqualTo(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS + " is not supported when SASL is terminated at the proxy");
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

    // --- computeSessionExpiry ---

    @Test
    void shouldReturnNullWhenBothExpiriesAreNull() {
        // Given
        var filter = createFilterWithMaxReauth(null);

        // When
        Instant result = filter.computeSessionExpiry(null);

        // Then
        assertThat(result).isNull();
    }

    @Test
    void shouldReturnMaxReauthExpiryWhenMechanismExpiryIsNull() {
        // Given
        var filter = createFilterWithMaxReauth(Duration.ofSeconds(5));

        // When
        Instant result = filter.computeSessionExpiry(null);

        // Then
        assertThat(result).isEqualTo(FIXED_INSTANT.plusSeconds(5));
    }

    @Test
    void shouldReturnMechanismExpiryWhenMaxReauthIsNull() {
        // Given
        var filter = createFilterWithMaxReauth(null);
        Instant mechanismExpiry = FIXED_INSTANT.plusSeconds(3);

        // When
        Instant result = filter.computeSessionExpiry(mechanismExpiry);

        // Then
        assertThat(result).isEqualTo(mechanismExpiry);
    }

    @Test
    void shouldReturnEarlierOfMaxReauthAndMechanismExpiryWhenMechanismIsEarlier() {
        // Given
        var filter = createFilterWithMaxReauth(Duration.ofSeconds(5));
        Instant mechanismExpiry = FIXED_INSTANT.plusSeconds(3);

        // When
        Instant result = filter.computeSessionExpiry(mechanismExpiry);

        // Then
        assertThat(result).isEqualTo(mechanismExpiry);
    }

    @Test
    void shouldReturnEarlierOfMaxReauthAndMechanismExpiryWhenMaxReauthIsEarlier() {
        // Given
        var filter = createFilterWithMaxReauth(Duration.ofSeconds(3));
        Instant mechanismExpiry = FIXED_INSTANT.plusSeconds(5);

        // When
        Instant result = filter.computeSessionExpiry(mechanismExpiry);

        // Then
        assertThat(result).isEqualTo(FIXED_INSTANT.plusSeconds(3));
    }

    // --- Helper methods ---

    private static SaslTerminationFilter createFilter() {
        var callbackHandler = new OAuthBearerValidatorCallbackHandler();
        var context = new SaslTermination.SaslTerminationContext(
                callbackHandler, OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES, Map.of(), Map.of(),
                Set.of("OAUTHBEARER"), List.of(),
                null, Clock.systemUTC(), Duration.ofMillis(200),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        return new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);
    }

    private static SaslTerminationFilter createFilterWithZeroDelay() {
        var context = new SaslTermination.SaslTerminationContext(
                null, OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES, Map.of(), Map.of(), Set.of("OAUTHBEARER"), List.of(),
                null, Clock.systemUTC(), Duration.ZERO,
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        return new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);
    }

    private static SaslTerminationFilter createFilterWithZeroDelayAndClock(Clock clock) {
        var context = new SaslTermination.SaslTerminationContext(
                null, OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES, Map.of(), Map.of(), Set.of("OAUTHBEARER"), List.of(),
                null, clock, Duration.ZERO,
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        return new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);
    }

    private static SaslTerminationFilter createFilterWithMaxReauth(@Nullable Duration maxReauth) {
        var context = new SaslTermination.SaslTerminationContext(
                null, OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES, Map.of(), Map.of(), Set.of("OAUTHBEARER"), List.of(),
                maxReauth, FIXED_CLOCK, Duration.ZERO,
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        return new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);
    }

    private static SaslTerminationFilter createFilterWithScram() {
        var credentialStore = mock(ScramCredentialStore.class);
        when(credentialStore.phantomSaltKey()).thenReturn(new byte[32]);
        var context = new SaslTermination.SaslTerminationContext(
                null, OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES,
                Map.of(ScramMechanism.SCRAM_SHA_256, credentialStore),
                Map.of(ScramMechanism.SCRAM_SHA_256, ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS),
                Set.of("SCRAM-SHA-256"), List.of(),
                null, Clock.systemUTC(), Duration.ofMillis(200),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        return new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);
    }

    private static SaslTerminationFilter createFilterWithScram512() {
        var credentialStore = mock(ScramCredentialStore.class);
        when(credentialStore.phantomSaltKey()).thenReturn(new byte[32]);
        var context = new SaslTermination.SaslTerminationContext(
                null, OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES,
                Map.of(ScramMechanism.SCRAM_SHA_512, credentialStore),
                Map.of(ScramMechanism.SCRAM_SHA_512, ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS),
                Set.of("SCRAM-SHA-512"), List.of(),
                null, Clock.systemUTC(), Duration.ofMillis(200),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        return new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);
    }

    private static SaslTerminationFilter createFilterWithClock(Clock clock) {
        var callbackHandler = new OAuthBearerValidatorCallbackHandler();
        var context = new SaslTermination.SaslTerminationContext(
                callbackHandler, OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES, Map.of(), Map.of(),
                Set.of("OAUTHBEARER"), List.of(),
                null, clock, Duration.ofMillis(200),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        return new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);
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
    private static FilterContext mockErrorFilterContextWithClose(ArgumentCaptor<Errors> errorsCaptor) {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var terminal = mock(TerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(filterContext.sessionId()).thenReturn("test-session");
        when(filterContext.getVirtualClusterName()).thenReturn(TEST_VIRTUAL_CLUSTER);
        when(builder.errorResponse(any(), any(), errorsCaptor.capture())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.withCloseConnection()).thenReturn(terminal);
        when(terminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockErrorFilterContextWithoutClose(ArgumentCaptor<Errors> errorsCaptor, ArgumentCaptor<String> messageCaptor) {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(filterContext.sessionId()).thenReturn("test-session");
        when(builder.errorResponse(any(), any(), errorsCaptor.capture(), messageCaptor.capture())).thenReturn(closeOrTerminal);
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
        filter.forceState(state);
    }
}
