/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import static io.kroxylicious.filter.sasl.termination.SaslTermination.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SaslTermination} filter factory.
 */
class SaslTerminationTest {

    private static final URI JWKS_URL = URI.create("https://idp.example.com/.well-known/jwks.json");

    private static OauthBearerMechanismConfig oauthConfig() {
        return new OauthBearerMechanismConfig(JWKS_URL, "kafka", "https://idp.example.com",
                null, null, null, null, null);
    }

    private static SaslTermination.SaslTerminationContext testContext() {
        return testContext(Set.of("OAUTHBEARER"), List.of());
    }

    private static SaslTermination.SaslTerminationContext testContext(
                                                                      Set<String> supportedMechanisms,
                                                                      List<AutoCloseable> closeables) {
        return new SaslTermination.SaslTerminationContext(
                null, supportedMechanisms, closeables, null, Clock.systemUTC(), Duration.ofMillis(200),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
    }

    @Test
    void shouldCloseCloseableOnFactoryClose() throws Exception {
        // Given
        var closeable = mock(AutoCloseable.class);
        var context = testContext(Set.of("OAUTHBEARER"), List.of(closeable));

        var factory = new SaslTermination();

        // When
        factory.close(context);

        // Then
        verify(closeable).close();
    }

    @Test
    void shouldCloseAllCloseablesOnFactoryClose() throws Exception {
        // Given
        var closeable1 = mock(AutoCloseable.class);
        var closeable2 = mock(AutoCloseable.class);
        var context = testContext(Set.of("OAUTHBEARER"), List.of(closeable1, closeable2));

        var saslTermination = new SaslTermination();

        // When
        saslTermination.close(context);

        // Then
        verify(closeable1).close();
        verify(closeable2).close();
    }

    @Test
    void shouldSuppressExceptionsWhenClosing() throws Exception {
        // Given
        var closeable = mock(AutoCloseable.class);
        RuntimeException exception = new RuntimeException("Failed to close");
        doThrow(exception).when(closeable).close();

        var context = testContext(Set.of("OAUTHBEARER"), List.of(closeable));

        var factory = new SaslTermination();

        // When/Then
        assertThatThrownBy(() -> factory.close(context))
                .isSameAs(exception);
    }

    @Test
    void shouldCreateFilterFromContext() {
        // Given
        var context = testContext();
        var filterFactoryContext = mock(FilterFactoryContext.class);

        var factory = new SaslTermination();

        // When
        var filter = factory.createFilter(filterFactoryContext, context);

        // Then
        assertThat(filter).isNotNull();
    }

    @Test
    void shouldRejectEmptyMechanismsList() {
        // When/Then
        assertThatThrownBy(() -> new SaslTerminationConfig(List.of(), null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one mechanism must be configured");
    }

    @Test
    void shouldRejectDuplicateMechanisms() {
        // Given
        var config1 = oauthConfig();
        var config2 = oauthConfig();

        // When/Then
        assertThatThrownBy(() -> new SaslTerminationConfig(List.of(config1, config2), null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate mechanism: OAUTHBEARER");
    }

    @Test
    void shouldDeserializeOauthBearerConfigFromJson() throws Exception {
        // Given
        String json = """
                {
                  "mechanisms": [
                    {
                      "mechanism": "OAUTHBEARER",
                      "jwksEndpointUrl": "https://idp.example.com/.well-known/jwks.json",
                      "expectedAudience": "kafka",
                      "expectedIssuer": "https://idp.example.com"
                    }
                  ]
                }
                """;

        // When
        var config = ConfigParser.createBaseObjectMapper().readValue(json, SaslTerminationConfig.class);

        // Then
        assertThat(config.mechanisms()).hasSize(1);
        assertThat(config.mechanisms().get(0)).isInstanceOf(OauthBearerMechanismConfig.class);
        assertThat(config.mechanisms().get(0).mechanismName()).isEqualTo("OAUTHBEARER");
    }

    @Test
    void shouldDeserializeOauthBearerOptionalDurationFieldsFromJson() throws Exception {
        // Given
        String json = """
                {
                  "mechanisms": [
                    {
                      "mechanism": "OAUTHBEARER",
                      "jwksEndpointUrl": "https://idp.example.com/.well-known/jwks.json",
                      "expectedAudience": "kafka",
                      "expectedIssuer": "https://idp.example.com",
                      "jwksEndpointRefresh": "5m",
                      "jwksEndpointRetryBackoff": "1s",
                      "jwksEndpointRetryBackoffMax": "10s"
                    }
                  ]
                }
                """;

        // When
        var config = ConfigParser.createBaseObjectMapper().readValue(json, SaslTerminationConfig.class);

        // Then
        assertThat(config.mechanisms().get(0)).isInstanceOf(OauthBearerMechanismConfig.class);
        var oauth = (OauthBearerMechanismConfig) config.mechanisms().get(0);
        assertThat(oauth.jwksEndpointRefresh()).isEqualTo(Duration.ofMinutes(5));
        assertThat(oauth.jwksEndpointRetryBackoff()).isEqualTo(Duration.ofSeconds(1));
        assertThat(oauth.jwksEndpointRetryBackoffMax()).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void shouldPassDurationFieldsToOauthSaslConfigMapAsMillis() {
        // Given
        var config = new OauthBearerMechanismConfig(
                URI.create("https://idp.example.com/.well-known/jwks.json"),
                "kafka", "https://idp.example.com",
                null, null,
                Duration.ofMinutes(5), Duration.ofSeconds(1), Duration.ofSeconds(10));

        // When
        var saslConfig = SaslTermination.createOauthSaslConfigMap(config);

        // Then
        assertThat(saslConfig)
                .containsEntry(org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, 300_000L)
                .containsEntry(org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, 1_000L)
                .containsEntry(org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, 10_000L);
    }

    @Test
    void shouldUseDefaultsWhenDurationFieldsAreNull() {
        // Given
        var config = new OauthBearerMechanismConfig(
                URI.create("https://idp.example.com/.well-known/jwks.json"),
                "kafka", "https://idp.example.com",
                null, null, null, null, null);

        // When
        var saslConfig = SaslTermination.createOauthSaslConfigMap(config);

        // Then
        assertThat(saslConfig)
                .containsEntry(org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS,
                        org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS)
                .containsEntry(org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS,
                        org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS)
                .containsEntry(org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS,
                        org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS);
    }

    @Test
    void addAllowedUrlShouldAddToSystemProperty() throws Exception {
        // Given
        var jwksUrl = "https://" + UUID.randomUUID() + ".invalid/jwks";
        var closeables = new ArrayList<AutoCloseable>();

        // When
        SaslTermination.addAllowedSaslOauthbearerUrl(jwksUrl, closeables);

        // Then
        try {
            assertThat(System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG))
                    .contains(jwksUrl);
        }
        finally {
            closeAll(closeables);
        }
    }

    @Test
    void allowedUrlCloseableShouldRemoveFromSystemProperty() throws Exception {
        // Given
        var jwksUrl = "https://" + UUID.randomUUID() + ".invalid/jwks";
        var closeables = new ArrayList<AutoCloseable>();
        SaslTermination.addAllowedSaslOauthbearerUrl(jwksUrl, closeables);

        // When
        closeAll(closeables);

        // Then
        assertThat(System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG))
                .satisfiesAnyOf(
                        v -> assertThat(v).isNull(),
                        v -> assertThat(v).doesNotContain(jwksUrl));
    }

    @Test
    void allowedUrlCloseableShouldNotRemoveWhileAnotherReferenceExists() throws Exception {
        // Given
        var jwksUrl = "https://" + UUID.randomUUID() + ".invalid/jwks";
        var closeables1 = new ArrayList<AutoCloseable>();
        var closeables2 = new ArrayList<AutoCloseable>();
        SaslTermination.addAllowedSaslOauthbearerUrl(jwksUrl, closeables1);
        SaslTermination.addAllowedSaslOauthbearerUrl(jwksUrl, closeables2);

        try {
            // When
            closeAll(closeables1);

            // Then
            assertThat(System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG))
                    .contains(jwksUrl);
        }
        finally {
            closeAll(closeables2);
        }
    }

    @Test
    void allowedUrlShouldBeRemovedOnceAllReferencesAreClosed() throws Exception {
        // Given
        var jwksUrl = "https://" + UUID.randomUUID() + ".invalid/jwks";
        var closeables1 = new ArrayList<AutoCloseable>();
        var closeables2 = new ArrayList<AutoCloseable>();
        SaslTermination.addAllowedSaslOauthbearerUrl(jwksUrl, closeables1);
        SaslTermination.addAllowedSaslOauthbearerUrl(jwksUrl, closeables2);

        // When
        closeAll(closeables1);
        closeAll(closeables2);

        // Then
        assertThat(System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG))
                .satisfiesAnyOf(
                        v -> assertThat(v).isNull(),
                        v -> assertThat(v).doesNotContain(jwksUrl));
    }

    private static void closeAll(List<AutoCloseable> closeables) throws Exception {
        for (var closeable : closeables) {
            closeable.close();
        }
    }

    @Test
    void effectiveFixedAuthDelayShouldDefaultTo200ms() {
        // Given
        var config = new SaslTerminationConfig(List.of(oauthConfig()), null, null, null, null);

        // When/Then
        assertThat(config.effectiveFixedAuthDelay()).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    void effectiveFixedAuthDelayShouldUseConfiguredValue() {
        // Given
        var config = new SaslTerminationConfig(List.of(oauthConfig()), null, Duration.ofMillis(500), null, null);

        // When/Then
        assertThat(config.effectiveFixedAuthDelay()).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void effectiveFixedAuthDelayShouldSupportZeroToDisable() {
        // Given
        var config = new SaslTerminationConfig(List.of(oauthConfig()), null, Duration.ZERO, null, null);

        // When/Then
        assertThat(config.effectiveFixedAuthDelay()).isEqualTo(Duration.ZERO);
    }

    @Test
    void shouldRejectSaslHandshakeWithUnsupportedApiVersion() throws Exception {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(java.util.concurrent.ScheduledExecutorService.class), context);

        var filterContext = mockFilterContextForErrorResponse();
        short unsupportedVersion = (short) (ApiKeys.SASL_HANDSHAKE.latestVersion() + 1);

        // When
        filter.onRequest(ApiKeys.SASL_HANDSHAKE, unsupportedVersion,
                new RequestHeaderData(), new SaslHandshakeRequestData(), filterContext);

        // Then
        verify(filterContext).requestFilterResultBuilder();
    }

    @Test
    void shouldRejectSaslAuthenticateWithUnsupportedApiVersion() throws Exception {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(java.util.concurrent.ScheduledExecutorService.class), context);

        var filterContext = mockFilterContextForErrorResponse();
        short unsupportedVersion = (short) (ApiKeys.SASL_AUTHENTICATE.latestVersion() + 1);

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, unsupportedVersion,
                new RequestHeaderData(), new SaslAuthenticateRequestData(), filterContext);

        // Then
        verify(filterContext).requestFilterResultBuilder();
    }

    @Test
    void shouldHandleRequestReturnsTrueInRequiringHandshakeState() {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(java.util.concurrent.ScheduledExecutorService.class), context);

        // When/Then
        assertThat(filter.shouldHandleRequest(ApiKeys.PRODUCE, (short) 0)).isTrue();
    }

    @Test
    void shouldHandleRequestReturnsFalseWhenAuthenticatedWithNoExpiry() {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(java.util.concurrent.ScheduledExecutorService.class), context);

        var start = State.start();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = start.nextState(handler, 0L);
        var authenticated = authenticating.nextStateSuccess("alice", "OAUTHBEARER", null);

        setFilterState(filter, authenticated);

        // When/Then
        assertThat(filter.shouldHandleRequest(ApiKeys.PRODUCE, (short) 0)).isFalse();
        assertThat(filter.shouldHandleRequest(ApiKeys.FETCH, (short) 0)).isFalse();
        assertThat(filter.shouldHandleRequest(ApiKeys.API_VERSIONS, (short) 0)).isTrue();
        assertThat(filter.shouldHandleRequest(ApiKeys.SASL_HANDSHAKE, (short) 0)).isTrue();
        assertThat(filter.shouldHandleRequest(ApiKeys.SASL_AUTHENTICATE, (short) 0)).isTrue();
    }

    @Test
    void shouldHandleRequestReturnsTrueWhenAuthenticatedWithExpiry() {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(java.util.concurrent.ScheduledExecutorService.class), context);

        var start = State.start();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = start.nextState(handler, 0L);
        var authenticated = authenticating.nextStateSuccess("alice", "OAUTHBEARER", java.time.Instant.now().plusSeconds(3600));

        setFilterState(filter, authenticated);

        // When/Then — with expiry set, all requests should be handled (for expiry checking)
        assertThat(filter.shouldHandleRequest(ApiKeys.PRODUCE, (short) 0)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = {
            "CREATE_DELEGATION_TOKEN", "RENEW_DELEGATION_TOKEN",
            "EXPIRE_DELEGATION_TOKEN", "DESCRIBE_DELEGATION_TOKEN"
    })
    void shouldRejectUnsupportedApiRequests(ApiKeys apiKey) throws Exception {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(java.util.concurrent.ScheduledExecutorService.class), context);

        var filterContext = mockFilterContextForErrorResponseWithoutClose();

        // When
        filter.onRequest(apiKey, apiKey.latestVersion(),
                new RequestHeaderData(), apiKey.messageType.newRequest(), filterContext);

        // Then
        verify(filterContext).requestFilterResultBuilder();
    }

    @Test
    void shouldRemoveDelegationTokenApisFromApiVersionsResponse() throws Exception {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(java.util.concurrent.ScheduledExecutorService.class), context);

        var apiKeys = new ArrayList<>(List.of(
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.PRODUCE.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.FETCH.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.CREATE_DELEGATION_TOKEN.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.RENEW_DELEGATION_TOKEN.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.EXPIRE_DELEGATION_TOKEN.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.DESCRIBE_DELEGATION_TOKEN.id)));
        var response = new ApiVersionsResponseData();
        response.apiKeys().addAll(apiKeys);

        var filterContext = mock(FilterContext.class);
        var responseResult = mock(ResponseFilterResult.class);
        when(filterContext.forwardResponse(any(), any())).thenReturn(CompletableFuture.completedFuture(responseResult));

        // When
        filter.onApiVersionsResponse((short) 0, new ResponseHeaderData(), response, filterContext);

        // Then
        var remainingApiKeys = response.apiKeys().stream()
                .map(ApiVersionsResponseData.ApiVersion::apiKey)
                .toList();
        assertThat(remainingApiKeys).containsExactly(
                ApiKeys.PRODUCE.id,
                ApiKeys.FETCH.id);
    }

    @Test
    void shouldHandleApiVersionsResponseWithNoTargetApis() throws Exception {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(java.util.concurrent.ScheduledExecutorService.class), context);

        var response = new ApiVersionsResponseData();
        response.apiKeys().add(new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.PRODUCE.id));

        var filterContext = mock(FilterContext.class);
        var responseResult = mock(ResponseFilterResult.class);
        when(filterContext.forwardResponse(any(), any())).thenReturn(CompletableFuture.completedFuture(responseResult));

        // When
        filter.onApiVersionsResponse((short) 0, new ResponseHeaderData(), response, filterContext);

        // Then
        var remainingKeys = response.apiKeys().stream()
                .map(ApiVersionsResponseData.ApiVersion::apiKey)
                .toList();
        assertThat(remainingKeys).containsExactly(ApiKeys.PRODUCE.id);
    }

    @Test
    void shouldRejectOversizedOauthBearerAuthBytes() throws Exception {
        // Given
        int maxAuthBytes = 128 * 1024;
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(maxAuthBytes);
        var context = testContext(Set.of("OAUTHBEARER"), List.of());
        var filter = new SaslTerminationFilter(mock(java.util.concurrent.ScheduledExecutorService.class), context);

        var authenticating = State.start().nextState(handler, 0L);
        setFilterState(filter, authenticating);

        byte[] oversizedPayload = new byte[maxAuthBytes + 1];
        var request = new SaslAuthenticateRequestData().setAuthBytes(oversizedPayload);
        var filterContext = mockFilterContextForShortCircuitWithClose();

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, (short) 0,
                new RequestHeaderData(), request, filterContext);

        // Then
        verify(filterContext).requestFilterResultBuilder();
    }

    @Test
    void fixedAuthDelayShouldCompleteOnProvidedExecutor() throws Exception {
        // Given
        var executorThreadName = "test-filter-dispatch";
        var executor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, executorThreadName));
        try {
            var context = new SaslTermination.SaslTerminationContext(
                    null, Set.of("OAUTHBEARER"), List.of(), null, Clock.systemUTC(),
                    Duration.ofMillis(100), SaslTermination.DEFAULT_SUBJECT_BUILDER);
            var filter = new SaslTerminationFilter(executor, context);

            var handler = mock(MechanismStateMachine.class);
            when(handler.mechanismName()).thenReturn("OAUTHBEARER");
            when(handler.maxAuthBytes()).thenReturn(128 * 1024);
            when(handler.evaluateRound(any())).thenReturn(
                    CompletableFuture.completedFuture(
                            RoundResult.failure(new byte[0], new javax.security.sasl.SaslException("test"))));

            var authenticating = State.start().nextState(handler, System.nanoTime());
            setFilterState(filter, authenticating);

            var request = new SaslAuthenticateRequestData().setAuthBytes(new byte[0]);
            var filterContext = mockFilterContextForShortCircuitWithClose();

            // When
            var completingThread = new java.util.concurrent.atomic.AtomicReference<String>();
            filter.onRequest(ApiKeys.SASL_AUTHENTICATE, (short) 0,
                    new RequestHeaderData(), request, filterContext)
                    .thenApply(result -> {
                        completingThread.set(Thread.currentThread().getName());
                        return result;
                    })
                    .toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);

            // Then
            assertThat(completingThread.get()).isEqualTo(executorThreadName);
        }
        finally {
            executor.shutdownNow();
        }
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

    @SuppressWarnings("unchecked")
    private static FilterContext mockFilterContextForShortCircuitWithClose() {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage.class);
        var terminal = mock(io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(builder.shortCircuitResponse(any())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.withCloseConnection()).thenReturn(terminal);
        when(terminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockFilterContextForShortCircuitResponse() {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(builder.shortCircuitResponse(any())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockFilterContextForErrorResponse() {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage.class);
        var terminal = mock(io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(builder.errorResponse(any(), any(), any())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.withCloseConnection()).thenReturn(terminal);
        when(terminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockFilterContextForErrorResponseWithoutClose() {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(builder.errorResponse(any(), any(), any())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }
}
