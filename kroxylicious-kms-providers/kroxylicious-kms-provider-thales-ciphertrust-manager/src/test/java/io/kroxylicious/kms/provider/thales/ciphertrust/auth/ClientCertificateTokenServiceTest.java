/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.auth;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;

import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthResponse;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ClientCertificateTokenService}.
 */
class ClientCertificateTokenServiceTest {

    public static final UnaryOperator<HttpClient.Builder> NOOP_TLS_CONFIGURATOR = builder -> builder;
    private static final String TEST_CLIENT_ID = "test-client-1";
    private static final String INITIAL_JWT = "initial-jwt-token";
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration ASYNC_TIMEOUT = Duration.ofSeconds(5);
    private static final URI VALID_ENDPOINT = URI.create("http://localhost");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static WireMockServer server;
    private ClientCertificateTokenService service;

    @BeforeAll
    static void initMockServer() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
    }

    @AfterAll
    static void shutdownMockServer() {
        server.shutdown();
    }

    @BeforeEach
    void beforeEach() {
        server.resetAll();
        URI endpoint = URI.create(server.baseUrl());
        service = new ClientCertificateTokenService(
                endpoint,
                TEST_CLIENT_ID,
                TEST_TIMEOUT,
                builder -> builder); // No TLS configuration for test
    }

    @AfterEach
    void afterEach() {
        if (service != null) {
            service.close();
        }
    }

    @Test
    @SuppressWarnings("resource")
    void constructorRejectsNullEndpoint() {
        // Given/When/Then
        assertThatThrownBy(() -> new ClientCertificateTokenService(null, TEST_CLIENT_ID, TEST_TIMEOUT, NOOP_TLS_CONFIGURATOR))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("endpointUrl cannot be null");
    }

    @Test
    @SuppressWarnings("resource")
    void constructorRejectsNullClientId() {
        // Given/When/Then
        assertThatThrownBy(() -> new ClientCertificateTokenService(VALID_ENDPOINT, null, TEST_TIMEOUT, NOOP_TLS_CONFIGURATOR))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("clientId cannot be null");
    }

    @Test
    @SuppressWarnings("resource")
    void constructorRejectsBlankClientId() {
        // Given/When/Then
        assertThatThrownBy(() -> new ClientCertificateTokenService(VALID_ENDPOINT, "", TEST_TIMEOUT, NOOP_TLS_CONFIGURATOR))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clientId cannot be blank");
    }

    @Test
    @SuppressWarnings("resource")
    void constructorRejectsNullTimeout() {
        // Given/When/Then
        assertThatThrownBy(() -> new ClientCertificateTokenService(VALID_ENDPOINT, TEST_CLIENT_ID, null, NOOP_TLS_CONFIGURATOR))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("timeout cannot be null");
    }

    @Test
    @SuppressWarnings("resource")
    void constructorRejectsNullTlsConfigurator() {
        // Given/When/Then
        assertThatThrownBy(() -> new ClientCertificateTokenService(VALID_ENDPOINT, TEST_CLIENT_ID, TEST_TIMEOUT, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("tlsConfigurator cannot be null");
    }

    @Test
    void authenticationSucceeds() {
        // Given
        stubClientCertAuth(INITIAL_JWT);

        // When
        var tokenStage = service.getBearerToken();

        // Then
        assertThat(tokenStage)
                .succeedsWithin(ASYNC_TIMEOUT)
                .satisfies(token -> {
                    assertThat(token.token()).isEqualTo(INITIAL_JWT);
                    assertThat(token.expires()).isAfter(token.created());
                });

        // Verify client_credentials grant was used
        server.verify(postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(matchingClientCredentialAuth()));
    }

    @Test
    void subsequentAuthUsesClientCredentials() {
        // Given
        stubClientCertAuth(INITIAL_JWT);
        assertThat(service.getBearerToken())
                .succeedsWithin(ASYNC_TIMEOUT)
                .satisfies(token -> assertThat(token.token()).isEqualTo(INITIAL_JWT));
        server.resetRequests();

        // When
        var secondTokenStage = service.getBearerToken();

        // Then
        assertThat(secondTokenStage)
                .succeedsWithin(ASYNC_TIMEOUT)
                .satisfies(token -> assertThat(token.token()).isEqualTo(INITIAL_JWT));

        // Verify client_credentials was used again (no refresh token)
        server.verify(postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(matchingClientCredentialAuth()));
    }

    @Test
    void authenticationFailsWithInvalidClient() {
        // Given
        stubClientCertAuthFailure();

        // When
        var tokenStage = service.getBearerToken();

        // Then
        assertThat(tokenStage)
                .failsWithin(ASYNC_TIMEOUT)
                .withThrowableThat()
                .havingRootCause()
                .withMessageContaining("client certificate authentication failed with HTTP 401");
    }

    @Test
    void authenticationFailsWithInvalidJsonResponse() {
        // Given
        stubClientCertAuthWithInvalidJson();

        // When
        var tokenStage = service.getBearerToken();

        // Then
        assertThat(tokenStage)
                .failsWithin(ASYNC_TIMEOUT)
                .withThrowableThat()
                .withMessageContaining("Failed to parse authentication response");
    }

    // Helper methods for stubbing

    private void stubClientCertAuth(String jwt) {
        // Note: Client certificate auth does NOT return a refresh_token
        AuthResponse response = new AuthResponse(jwt, 300, null);

        server.stubFor(
                post(urlPathEqualTo("/api/v1/auth/tokens/"))
                        .withRequestBody(matchingClientCredentialAuth())
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(toJson(response))));
    }

    private void stubClientCertAuthFailure() {
        String errorResponse = """
                {
                    "code": 5,
                    "codeDesc": "NCERRUnauthorizedAccess",
                    "message": "Invalid Client",
                    "requestID": "test-request-id"
                }
                """;

        server.stubFor(
                post(urlPathEqualTo("/api/v1/auth/tokens/"))
                        .withRequestBody(matchingClientCredentialAuth())
                        .willReturn(aResponse()
                                .withStatus(401)
                                .withHeader("Content-Type", "application/json")
                                .withBody(errorResponse)));
    }

    private void stubClientCertAuthWithInvalidJson() {
        String invalidResponse = "This is not JSON";

        server.stubFor(
                post(urlPathEqualTo("/api/v1/auth/tokens/"))
                        .withRequestBody(matchingClientCredentialAuth())
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(invalidResponse)));
    }

    // Helper methods for request matching

    private StringValuePattern matchingClientCredentialAuth() {
        return containing("\"grant_type\":\"client_credential\"")
                .and(containing("\"client_id\":\"" + TEST_CLIENT_ID + "\""));
    }

    private String toJson(Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize object to JSON", e);
        }
    }
}
