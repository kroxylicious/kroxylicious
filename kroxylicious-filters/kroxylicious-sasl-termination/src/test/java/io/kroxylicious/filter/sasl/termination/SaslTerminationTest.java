/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;

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

    @Test
    void shouldCloseHandlerFactoryOnFactoryClose() {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);

        var factory = new SaslTermination();

        // When
        factory.close(context);

        // Then
        verify(handlerFactory).close();
    }

    @Test
    void shouldCloseAllHandlerFactoriesOnFactoryClose() {
        // Given
        var factory1 = mock(MechanismHandlerFactory.class);
        var factory2 = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", factory1, "SCRAM-SHA-512", factory2), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);

        var saslTermination = new SaslTermination();

        // When
        saslTermination.close(context);

        // Then
        verify(factory1).close();
        verify(factory2).close();
    }

    @Test
    void shouldSuppressExceptionsWhenClosingFactory() {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        RuntimeException exception = new RuntimeException("Factory failed to close");
        doThrow(exception).when(handlerFactory).close();

        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);

        var factory = new SaslTermination();

        // When/Then
        assertThatThrownBy(() -> factory.close(context))
                .isSameAs(exception);
    }

    @Test
    void shouldCreateFilterFromContext() {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
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
        var config1 = new ScramSha256MechanismConfig("store1", new Object());
        var config2 = new ScramSha256MechanismConfig("store2", new Object());

        // When/Then
        assertThatThrownBy(() -> new SaslTerminationConfig(List.of(config1, config2), null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate mechanism: SCRAM-SHA-256");
    }

    @Test
    void shouldAcceptMultipleDistinctMechanisms() {
        // Given
        var scram = new ScramSha256MechanismConfig("store", new Object());
        var oauth = new OauthBearerMechanismConfig(
                URI.create("https://example.com/jwks"), "aud", "iss",
                null, null, null, null, null);

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
                      "credentialStore": "KeystoreScramCredentialStoreService",
                      "credentialStoreConfig": {"file": "/path/to/creds.p12"}
                    }
                  ]
                }
                """;

        // When
        var config = new ObjectMapper().readValue(json, SaslTerminationConfig.class);

        // Then
        assertThat(config.mechanisms()).hasSize(1);
        assertThat(config.mechanisms().get(0)).isInstanceOf(ScramSha256MechanismConfig.class);
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
                      "credentialStore": "KeystoreScramCredentialStoreService",
                      "credentialStoreConfig": {"file": "/path/to/creds.p12"}
                    }
                  ]
                }
                """;

        // When
        var config = new ObjectMapper().readValue(json, SaslTerminationConfig.class);

        // Then
        assertThat(config.mechanisms()).hasSize(1);
        assertThat(config.mechanisms().get(0)).isInstanceOf(ScramSha512MechanismConfig.class);
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
        var config = new ObjectMapper().readValue(json, SaslTerminationConfig.class);

        // Then
        assertThat(config.mechanisms()).hasSize(1);
        assertThat(config.mechanisms().get(0)).isInstanceOf(OauthBearerMechanismConfig.class);
        assertThat(config.mechanisms().get(0).mechanismName()).isEqualTo("OAUTHBEARER");
    }

    @Test
    void shouldDeserializeMultipleMechanismsFromJson() throws Exception {
        // Given
        String json = """
                {
                  "mechanisms": [
                    {
                      "mechanism": "SCRAM-SHA-256",
                      "credentialStore": "KeystoreScramCredentialStoreService",
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
        var config = new ObjectMapper().readValue(json, SaslTerminationConfig.class);

        // Then
        assertThat(config.mechanisms()).hasSize(2);
        assertThat(config.mechanisms().get(0)).isInstanceOf(ScramSha256MechanismConfig.class);
        assertThat(config.mechanisms().get(1)).isInstanceOf(OauthBearerMechanismConfig.class);
    }

    @Test
    void effectiveFixedAuthDelayShouldDefaultTo200ms() {
        // Given
        var scram = new ScramSha256MechanismConfig("store", new Object());
        var config = new SaslTerminationConfig(List.of(scram), null, null, null, null);

        // When/Then
        assertThat(config.effectiveFixedAuthDelay()).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    void effectiveFixedAuthDelayShouldUseConfiguredValue() {
        // Given
        var scram = new ScramSha256MechanismConfig("store", new Object());
        var config = new SaslTerminationConfig(List.of(scram), null, Duration.ofMillis(500), null, null);

        // When/Then
        assertThat(config.effectiveFixedAuthDelay()).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void effectiveFixedAuthDelayShouldSupportZeroToDisable() {
        // Given
        var scram = new ScramSha256MechanismConfig("store", new Object());
        var config = new SaslTerminationConfig(List.of(scram), null, Duration.ZERO, null, null);

        // When/Then
        assertThat(config.effectiveFixedAuthDelay()).isEqualTo(Duration.ZERO);
    }

    @Test
    void shouldRejectSaslHandshakeWithUnsupportedApiVersion() throws Exception {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

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
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

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
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

        // When/Then
        assertThat(filter.shouldHandleRequest(ApiKeys.PRODUCE, (short) 0)).isTrue();
    }

    @Test
    void shouldHandleRequestReturnsFalseWhenAuthenticatedWithNoExpiry() {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

        // Manually transition to Authenticated state with no expiry
        var start = State.start();
        var handler = mock(io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandler.class);
        var authenticating = start.nextState(handler);
        var authenticated = authenticating.nextStateSuccess("alice", null);

        // Use reflection to set state - this is acceptable in tests
        try {
            var field = SaslTerminationFilter.class.getDeclaredField("state");
            field.setAccessible(true);
            field.set(filter, authenticated);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

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
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

        var start = State.start();
        var handler = mock(io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandler.class);
        var authenticating = start.nextState(handler);
        var authenticated = authenticating.nextStateSuccess("alice", java.time.Instant.now().plusSeconds(3600));

        try {
            var field = SaslTerminationFilter.class.getDeclaredField("state");
            field.setAccessible(true);
            field.set(filter, authenticated);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        // When/Then — with expiry set, all requests should be handled (for expiry checking)
        assertThat(filter.shouldHandleRequest(ApiKeys.PRODUCE, (short) 0)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = {
            "CREATE_DELEGATION_TOKEN", "RENEW_DELEGATION_TOKEN",
            "EXPIRE_DELEGATION_TOKEN", "DESCRIBE_DELEGATION_TOKEN",
            "ALTER_USER_SCRAM_CREDENTIALS"
    })
    void shouldRejectUnsupportedApiRequests(ApiKeys apiKey) throws Exception {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

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
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

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
                ApiKeys.FETCH.id,
                ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS.id);
    }

    @Test
    void shouldHandleApiVersionsResponseWithNoTargetApis() throws Exception {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

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
    void shouldDescribeExistingUser() throws Exception {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var credentialStore = mock(ScramCredentialStore.class);
        var credential = mock(io.kroxylicious.sasl.credentialstore.ScramCredential.class);
        when(credential.iterations()).thenReturn(10000);
        when(credentialStore.lookupCredential("alice"))
                .thenReturn(CompletableFuture.completedFuture(credential));

        byte mechanismType = 1;
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory),
                Map.of(mechanismType, credentialStore),
                null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

        var request = new DescribeUserScramCredentialsRequestData();
        request.users().add(new DescribeUserScramCredentialsRequestData.UserName().setName("alice"));

        var filterContext = mockFilterContextForShortCircuitResponse();

        // When
        filter.onRequest(ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS, (short) 0,
                new RequestHeaderData(), request, filterContext);

        // Then
        verify(credentialStore).lookupCredential("alice");
    }

    @Test
    void shouldDescribeNonExistentUser() throws Exception {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var credentialStore = mock(ScramCredentialStore.class);
        when(credentialStore.lookupCredential("unknown"))
                .thenReturn(CompletableFuture.completedFuture(null));

        byte mechanismType = 1;
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory),
                Map.of(mechanismType, credentialStore),
                null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

        var request = new DescribeUserScramCredentialsRequestData();
        request.users().add(new DescribeUserScramCredentialsRequestData.UserName().setName("unknown"));

        var filterContext = mockFilterContextForShortCircuitResponse();

        // When
        filter.onRequest(ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS, (short) 0,
                new RequestHeaderData(), request, filterContext);

        // Then
        verify(credentialStore).lookupCredential("unknown");
    }

    @Test
    void shouldRejectOversizedScramAuthBytes() throws Exception {
        // Given
        var handler = mock(io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandler.class);
        when(handler.mechanismName()).thenReturn("SCRAM-SHA-256");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

        var authenticating = State.start().nextState(handler);
        setFilterState(filter, authenticating);

        byte[] oversizedPayload = new byte[handler.maxAuthBytes() + 1];
        var request = new SaslAuthenticateRequestData().setAuthBytes(oversizedPayload);
        var filterContext = mockFilterContextForShortCircuitWithClose();

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, (short) 0,
                new RequestHeaderData(), request, filterContext);

        // Then
        verify(filterContext).requestFilterResultBuilder();
    }

    @Test
    void shouldRejectOversizedOauthBearerAuthBytes() throws Exception {
        // Given
        var handler = mock(io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandler.class);
        when(handler.mechanismName()).thenReturn("OAUTHBEARER");
        when(handler.maxAuthBytes()).thenReturn(128 * 1024);
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("OAUTHBEARER", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

        var authenticating = State.start().nextState(handler);
        setFilterState(filter, authenticating);

        byte[] oversizedPayload = new byte[handler.maxAuthBytes() + 1];
        var request = new SaslAuthenticateRequestData().setAuthBytes(oversizedPayload);
        var filterContext = mockFilterContextForShortCircuitWithClose();

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, (short) 0,
                new RequestHeaderData(), request, filterContext);

        // Then
        verify(filterContext).requestFilterResultBuilder();
    }

    @Test
    void shouldAcceptAuthBytesWithinScramLimit() throws Exception {
        // Given
        var handler = mock(io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandler.class);
        when(handler.mechanismName()).thenReturn("SCRAM-SHA-256");
        when(handler.maxAuthBytes()).thenReturn(4 * 1024);
        when(handler.handleAuthenticate(any())).thenReturn(
                CompletableFuture.completedFuture(
                        io.kroxylicious.filter.sasl.termination.mechanism.AuthenticationResult.failure(new byte[0], "test")));
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), Map.of(), null, java.time.Clock.systemUTC(),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(context);

        var authenticating = State.start().nextState(handler);
        setFilterState(filter, authenticating);

        byte[] payload = new byte[handler.maxAuthBytes()];
        var request = new SaslAuthenticateRequestData().setAuthBytes(payload);
        var filterContext = mockFilterContextForShortCircuitWithClose();

        // When
        filter.onRequest(ApiKeys.SASL_AUTHENTICATE, (short) 0,
                new RequestHeaderData(), request, filterContext);

        // Then
        verify(handler).handleAuthenticate(any());
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
