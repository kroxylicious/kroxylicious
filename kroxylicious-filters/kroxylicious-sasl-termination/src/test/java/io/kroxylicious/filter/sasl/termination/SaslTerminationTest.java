/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link SaslTermination} filter factory.
 */
class SaslTerminationTest {

    @Test
    void shouldCloseHandlerFactoryOnFactoryClose() {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), null, java.time.Clock.systemUTC());

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
                Map.of("SCRAM-SHA-256", factory1, "SCRAM-SHA-512", factory2), null, java.time.Clock.systemUTC());

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
                Map.of("SCRAM-SHA-256", handlerFactory), null, java.time.Clock.systemUTC());

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
                Map.of("SCRAM-SHA-256", handlerFactory), null, java.time.Clock.systemUTC());
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
        assertThatThrownBy(() -> new SaslTerminationConfig(List.of(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one mechanism must be configured");
    }

    @Test
    void shouldRejectDuplicateMechanisms() {
        // Given
        var config1 = new ScramSha256MechanismConfig("store1", new Object());
        var config2 = new ScramSha256MechanismConfig("store2", new Object());

        // When/Then
        assertThatThrownBy(() -> new SaslTerminationConfig(List.of(config1, config2), null))
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
        var config = new SaslTerminationConfig(List.of(scram, oauth), null);

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
}
