/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.sasl.SaslException;

import org.apache.kafka.common.config.SaslConfigs;
import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.channel.DefaultEventLoop;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage;
import io.kroxylicious.proxy.internal.NettyFilterDispatchExecutor;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramCredentialStore;

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

    private static SaslTermination.SaslTerminationContext testContext() {
        return testContext(Set.of("SCRAM-SHA-256"), Map.of(), List.of());
    }

    private static SaslTermination.SaslTerminationContext testContext(
                                                                      Set<String> supportedMechanisms,
                                                                      Map<ScramMechanism, ScramCredentialStore> scramStores,
                                                                      List<AutoCloseable> closeables) {
        return new SaslTermination.SaslTerminationContext(
                null,
                OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES,
                scramStores,
                Map.of(),
                supportedMechanisms,
                closeables,
                null,
                Clock.systemUTC(),
                Duration.ofMillis(200),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
    }

    @Test
    void shouldCloseCloseableOnFactoryClose() throws Exception {
        // Given
        var closeable = mock(AutoCloseable.class);
        var context = testContext(Set.of("SCRAM-SHA-256"), Map.of(), List.of(closeable));

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
        var context = testContext(Set.of("SCRAM-SHA-256"), Map.of(), List.of(closeable1, closeable2));

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

        var context = testContext(Set.of("SCRAM-SHA-256"), Map.of(), List.of(closeable));

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
        // Given
        List<MechanismConfig> mechanisms = List.of();

        // When/Then
        assertThatThrownBy(() -> new SaslTerminationConfig(mechanisms, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one mechanism must be configured");
    }

    @Test
    void shouldRejectDuplicateMechanisms() {
        // Given
        var config1 = new ScramMechanismConfig("SCRAM-SHA-256", "store1", new Object(), null);
        var config2 = new ScramMechanismConfig("SCRAM-SHA-256", "store2", new Object(), null);
        List<MechanismConfig> mechanisms = List.of(config1, config2);

        // When/Then
        assertThatThrownBy(() -> new SaslTerminationConfig(mechanisms, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate mechanism: SCRAM-SHA-256");
    }

    @Test
    void shouldAcceptMultipleDistinctMechanisms() {
        // Given
        var scram = new ScramMechanismConfig("SCRAM-SHA-256", "store", new Object(), null);
        var oauth = new OauthBearerMechanismConfig(
                URI.create("https://example.com/jwks"), "aud", "iss",
                null, null, null, null, null,
                OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES);

        // When
        var config = new SaslTerminationConfig(List.of(scram, oauth), null, null, null, null);

        // Then
        assertThat(config.mechanisms()).hasSize(2);
    }

    @Test
    void shouldDeserializeScramSha256ConfigFromJson() throws Exception {
        // Given
        String json = """
                {
                  "mechanisms": [
                    {
                      "mechanism": "SCRAM-SHA-256",
                      "credentialStore": "ScramCredentialFileService",
                      "credentialStoreConfig": {"file": "/path/to/creds.p12"}
                    }
                  ]
                }
                """;

        // When
        var config = ConfigParser.createBaseObjectMapper().readValue(json, SaslTerminationConfig.class);

        // Then
        assertThat(config.mechanisms()).hasSize(1);
        assertThat(config.mechanisms().get(0)).isInstanceOf(ScramMechanismConfig.class);
        assertThat(config.mechanisms().get(0).mechanismName()).isEqualTo("SCRAM-SHA-256");
    }

    @Test
    void shouldDeserializeScramSha512ConfigFromJson() throws Exception {
        // Given
        String json = """
                {
                  "mechanisms": [
                    {
                      "mechanism": "SCRAM-SHA-512",
                      "credentialStore": "ScramCredentialFileService",
                      "credentialStoreConfig": {"file": "/path/to/creds.p12"}
                    }
                  ]
                }
                """;

        // When
        var config = ConfigParser.createBaseObjectMapper().readValue(json, SaslTerminationConfig.class);

        // Then
        assertThat(config.mechanisms()).hasSize(1);
        assertThat(config.mechanisms().get(0)).isInstanceOf(ScramMechanismConfig.class);
        assertThat(config.mechanisms().get(0).mechanismName()).isEqualTo("SCRAM-SHA-512");
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
                Duration.ofMinutes(5), Duration.ofSeconds(1), Duration.ofSeconds(10), null);

        // When
        var saslConfig = SaslTermination.createOauthSaslConfigMap(config);

        // Then
        assertThat(saslConfig)
                .containsEntry(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, 300_000L)
                .containsEntry(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, 1_000L)
                .containsEntry(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, 10_000L);
    }

    @Test
    void shouldUseDefaultsWhenDurationFieldsAreNull() {
        // Given
        var config = new OauthBearerMechanismConfig(
                URI.create("https://idp.example.com/.well-known/jwks.json"),
                "kafka", "https://idp.example.com",
                null, null, null, null, null, null);

        // When
        var saslConfig = SaslTermination.createOauthSaslConfigMap(config);

        // Then
        assertThat(saslConfig)
                .containsEntry(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS,
                        SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS)
                .containsEntry(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS,
                        SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS)
                .containsEntry(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS,
                        SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS);
    }

    @ParameterizedTest
    @ValueSource(strings = { ",,,  ,", ",", "  ,  ,  " })
    void shouldRejectExpectedAudienceWithNoValidEntries(String audience) {
        // Given
        var config = new OauthBearerMechanismConfig(
                URI.create("https://idp.example.com/.well-known/jwks.json"),
                audience, "https://idp.example.com",
                null, null, null, null, null, null);

        // When / Then
        assertThatThrownBy(() -> SaslTermination.createOauthSaslConfigMap(config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("expectedAudience");
    }

    @Test
    void shouldDeserializeMultipleMechanismsFromJson() throws Exception {
        // Given
        String json = """
                {
                  "mechanisms": [
                    {
                      "mechanism": "SCRAM-SHA-256",
                      "credentialStore": "ScramCredentialFileService",
                      "credentialStoreConfig": {"file": "/path/to/creds.p12"}
                    },
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
        assertThat(config.mechanisms()).hasSize(2);
        assertThat(config.mechanisms().get(0)).isInstanceOf(ScramMechanismConfig.class);
        assertThat(config.mechanisms().get(1)).isInstanceOf(OauthBearerMechanismConfig.class);
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
        var scram = new ScramMechanismConfig("SCRAM-SHA-256", "store", new Object(), null);
        var config = new SaslTerminationConfig(List.of(scram), null, null, null, null);

        // When/Then
        assertThat(config.effectiveFixedAuthDelay()).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    void effectiveFixedAuthDelayShouldUseConfiguredValue() {
        // Given
        var scram = new ScramMechanismConfig("SCRAM-SHA-256", "store", new Object(), null);
        var config = new SaslTerminationConfig(List.of(scram), null, Duration.ofMillis(500), null, null);

        // When/Then
        assertThat(config.effectiveFixedAuthDelay()).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void effectiveFixedAuthDelayShouldSupportZeroToDisable() {
        // Given
        var scram = new ScramMechanismConfig("SCRAM-SHA-256", "store", new Object(), null);
        var config = new SaslTerminationConfig(List.of(scram), null, Duration.ZERO, null, null);

        // When/Then
        assertThat(config.effectiveFixedAuthDelay()).isEqualTo(Duration.ZERO);
    }

    @Test
    void shouldRejectNullCredentialStore() {
        // Given
        var storeConfig = new Object();

        // When/Then
        assertThatThrownBy(() -> new ScramMechanismConfig("SCRAM-SHA-256", null, storeConfig, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("credentialStore must not be null or empty");
    }

    @Test
    void shouldRejectEmptyCredentialStore() {
        // Given
        var storeConfig = new Object();

        // When/Then
        assertThatThrownBy(() -> new ScramMechanismConfig("SCRAM-SHA-256", "", storeConfig, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("credentialStore must not be null or empty");
    }

    @Test
    void shouldRejectNullCredentialStoreConfig() {
        // When/Then
        assertThatThrownBy(() -> new ScramMechanismConfig("SCRAM-SHA-256", "store", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("credentialStoreConfig must not be null");
    }

    @Test
    void shouldRejectPhantomIterationsBelowMinimum() {
        // Given
        var storeConfig = new Object();

        // When/Then
        assertThatThrownBy(() -> new ScramMechanismConfig("SCRAM-SHA-256", "store", storeConfig, ScramCredential.MINIMUM_ITERATIONS - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("phantomIterations must be at least");
    }

    @Test
    void shouldAcceptPhantomIterationsAtMinimum() {
        // Given/When/Then
        var config = new ScramMechanismConfig("SCRAM-SHA-256", "store", new Object(), ScramCredential.MINIMUM_ITERATIONS);
        assertThat(config.phantomIterations()).isEqualTo(ScramCredential.MINIMUM_ITERATIONS);
    }

    @Test
    void shouldUseDefaultPhantomIterationsWhenNull() {
        // Given
        var config = new ScramMechanismConfig("SCRAM-SHA-256", "store", new Object(), null);

        // When/Then
        assertThat(config.effectivePhantomIterations()).isEqualTo(ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS);
    }

    @Test
    void shouldUseConfiguredPhantomIterationsWhenSet() {
        // Given
        var config = new ScramMechanismConfig("SCRAM-SHA-256", "store", new Object(), 20_000);

        // When/Then
        assertThat(config.effectivePhantomIterations()).isEqualTo(20_000);
    }

    @Test
    void shouldRejectSaslHandshakeWithUnsupportedApiVersion() {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);

        var filterContext = mockFilterContextForErrorResponse();
        short unsupportedVersion = (short) (ApiKeys.SASL_HANDSHAKE.latestVersion() + 1);

        // When
        filter.onRequest(ApiKeys.SASL_HANDSHAKE, unsupportedVersion,
                new RequestHeaderData(), new SaslHandshakeRequestData(), filterContext);

        // Then
        verify(filterContext).requestFilterResultBuilder();
    }

    @Test
    void shouldRejectSaslAuthenticateWithUnsupportedApiVersion() {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);

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
        var filter = new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);

        // When/Then
        assertThat(filter.shouldHandleRequest(ApiKeys.PRODUCE, (short) 0)).isTrue();
    }

    @Test
    void shouldHandleRequestReturnsFalseWhenAuthenticatedWithNoExpiry() {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);

        var start = State.start();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = start.nextState(handler);
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
        var filter = new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);

        var start = State.start();
        var handler = mock(MechanismStateMachine.class);
        var authenticating = start.nextState(handler);
        var authenticated = authenticating.nextStateSuccess("alice", "OAUTHBEARER", Instant.now().plusSeconds(3600));

        setFilterState(filter, authenticated);

        // When/Then — with expiry set, all requests should be handled (for expiry checking)
        assertThat(filter.shouldHandleRequest(ApiKeys.PRODUCE, (short) 0)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = {
            "CREATE_DELEGATION_TOKEN", "RENEW_DELEGATION_TOKEN",
            "EXPIRE_DELEGATION_TOKEN", "DESCRIBE_DELEGATION_TOKEN",
            "ALTER_USER_SCRAM_CREDENTIALS", "DESCRIBE_USER_SCRAM_CREDENTIALS"
    })
    void shouldRejectUnsupportedApiRequests(ApiKeys apiKey) {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);
        var handler = mock(MechanismStateMachine.class);
        var authenticating = State.start().nextState(handler);
        setFilterState(filter, authenticating.nextStateSuccess("alice", "OAUTHBEARER", null));

        var filterContext = mockFilterContextForErrorResponseWithoutClose();

        // When
        filter.onRequest(apiKey, apiKey.latestVersion(),
                new RequestHeaderData(), apiKey.messageType.newRequest(), filterContext);

        // Then
        verify(filterContext).requestFilterResultBuilder();
    }

    @Test
    void shouldRemoveDelegationTokenApisFromApiVersionsResponse() {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);

        var apiKeys = new ArrayList<>(List.of(
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.PRODUCE.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.FETCH.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.CREATE_DELEGATION_TOKEN.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.RENEW_DELEGATION_TOKEN.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.EXPIRE_DELEGATION_TOKEN.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.DESCRIBE_DELEGATION_TOKEN.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS.id),
                new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS.id)));
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
    void shouldHandleApiVersionsResponseWithNoTargetApis() {
        // Given
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);

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
    void shouldRejectOversizedScramAuthBytes() {
        // Given
        int maxAuthBytes = 4 * 1024;
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("SCRAM-SHA-256");
        when(handler.maxAuthBytes()).thenReturn(maxAuthBytes);
        var context = testContext(Set.of("OAUTHBEARER"), Map.of(), List.of());
        var filter = new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);

        var authenticating = State.start().nextState(handler);
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
    void shouldRejectOversizedOauthBearerAuthBytes() {
        // Given
        int maxAuthBytes = 128 * 1024;
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(maxAuthBytes);
        var context = testContext(Set.of("OAUTHBEARER"), Map.of(), List.of());
        var filter = new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);

        var authenticating = State.start().nextState(handler);
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
    void shouldAcceptAuthBytesWithinScramLimit() {
        // Given
        int maxAuthBytes = 4 * 1024;
        var handler = mock(MechanismStateMachine.class);
        when(handler.mechanismName()).thenReturn("SCRAM-SHA-256");
        when(handler.maxAuthBytes()).thenReturn(maxAuthBytes);
        when(handler.evaluateRound(any())).thenReturn(
                CompletableFuture.completedFuture(
                        RoundResult.failure(new byte[0], new SaslException("test"))));
        var context = testContext();
        var filter = new SaslTerminationFilter(mock(FilterDispatchExecutor.class), context);

        var authenticating = State.start().nextState(handler);
        setFilterState(filter, authenticating);

        byte[] payload = new byte[maxAuthBytes];
        var request = new SaslAuthenticateRequestData().setAuthBytes(payload);
        var filterContext = mockFilterContextForShortCircuitWithClose();

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, (short) 0,
                new RequestHeaderData(), request, filterContext);

        // Then
        verify(handler).evaluateRound(any());
    }

    @SuppressWarnings("java:S2093") // The recommended try-with-resources for `executor` actually results in deadlock; it's closed with the eventLoop anyway
    @Test
    void fixedAuthDelayShouldCompleteOnProvidedExecutor() throws Exception {
        // Given
        var eventLoop = new DefaultEventLoop();
        try {
            var executor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            var eventLoopThread = eventLoop.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            var context = new SaslTermination.SaslTerminationContext(
                    null, OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES, Map.of(), Map.of(), Set.of("OAUTHBEARER"), List.of(), null, Clock.systemUTC(),
                    Duration.ofMillis(100), SaslTermination.DEFAULT_SUBJECT_BUILDER);
            var filter = new SaslTerminationFilter(executor, context);

            var handler = mock(MechanismStateMachine.class);
            when(handler.mechanismName()).thenReturn("SCRAM-SHA-256");
            when(handler.maxAuthBytes()).thenReturn(4 * 1024);
            when(handler.evaluateRound(any())).thenReturn(
                    CompletableFuture.completedFuture(
                            RoundResult.failure(new byte[0], new SaslException("test"))));

            var authenticating = State.start().nextState(handler);
            setFilterState(filter, authenticating);

            var request = new SaslAuthenticateRequestData().setAuthBytes(new byte[0]);
            var filterContext = mockFilterContextForShortCircuitWithClose();

            // When
            var completingThread = new AtomicReference<Thread>();
            filter.onRequest(ApiKeys.SASL_AUTHENTICATE, (short) 0,
                    new RequestHeaderData(), request, filterContext)
                    .thenApply(result -> {
                        completingThread.set(Thread.currentThread());
                        return result;
                    })
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);

            // Then
            assertThat(completingThread.get()).isEqualTo(eventLoopThread);
        }
        finally {
            eventLoop.shutdownGracefully().sync();
        }
    }

    private static void setFilterState(SaslTerminationFilter filter, State state) {
        filter.forceState(state);
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockFilterContextForShortCircuitWithClose() {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var terminal = mock(TerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(filterContext.getVirtualClusterName()).thenReturn("test-cluster");
        when(builder.shortCircuitResponse(any())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.withCloseConnection()).thenReturn(terminal);
        when(terminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockFilterContextForErrorResponse() {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var terminal = mock(TerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(builder.errorResponse(any(), any(), any(Errors.class))).thenReturn(closeOrTerminal);
        when(closeOrTerminal.withCloseConnection()).thenReturn(terminal);
        when(terminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockFilterContextForErrorResponseWithoutClose() {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(CloseOrTerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        when(builder.errorResponse(any(), any(), any(Errors.class), any())).thenReturn(closeOrTerminal);
        when(closeOrTerminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        return filterContext;
    }
}
