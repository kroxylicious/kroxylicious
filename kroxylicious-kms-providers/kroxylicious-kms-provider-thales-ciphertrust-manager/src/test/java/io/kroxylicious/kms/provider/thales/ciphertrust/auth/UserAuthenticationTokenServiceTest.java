/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.auth;

import java.net.URI;
import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;

import edu.umd.cs.findbugs.annotations.Nullable;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.not;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link UserAuthenticationTokenService} focusing on refresh token behavior.
 */
class UserAuthenticationTokenServiceTest {

    private static final String TEST_USERNAME = "testuser";
    private static final String TEST_PASSWORD = "testpass";
    private static final String INITIAL_JWT = "initial-jwt-token";
    private static final String INITIAL_REFRESH_TOKEN = "initial-refresh-token";
    private static final String REFRESHED_JWT = "refreshed-jwt-token";
    private static final String NEW_REFRESH_TOKEN = "new-refresh-token";

    private static WireMockServer server;
    private UserAuthenticationTokenService service;

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
        service = buildService(null);
    }

    private UserAuthenticationTokenService buildService(@Nullable String domain) {
        URI endpoint = URI.create(server.baseUrl());
        return new UserAuthenticationTokenService(
                endpoint,
                TEST_USERNAME,
                TEST_PASSWORD,
                domain,
                Duration.ofSeconds(10),
                builder -> builder);
    }

    @AfterEach
    void afterEach() {
        if (service != null) {
            service.close();
        }
    }

    @Test
    void initialAuthenticationCapturesRefreshToken() {
        // Given
        stubPasswordAuth(INITIAL_JWT, INITIAL_REFRESH_TOKEN);

        // When
        var tokenStage = service.getBearerToken();

        // Then
        assertThat(tokenStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .satisfies(token -> {
                    assertThat(token.token()).isEqualTo(INITIAL_JWT);
                    assertThat(token.expires()).isAfter(token.created());
                });

        // Verify password auth was used (with grant_type)
        server.verify(postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(matchingPasswordAuth()));
    }

    @Test
    void subsequentAuthUsesRefreshToken() {
        // Given
        stubPasswordAuth(INITIAL_JWT, INITIAL_REFRESH_TOKEN);
        stubRefreshTokenAuth(INITIAL_REFRESH_TOKEN, REFRESHED_JWT, NEW_REFRESH_TOKEN);

        // First call: password auth
        assertThat(service.getBearerToken())
                .succeedsWithin(Duration.ofSeconds(5));
        server.resetRequests();

        // When
        // Second call: should use refresh token
        var secondTokenStage = service.getBearerToken();

        // Then
        assertThat(secondTokenStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .satisfies(token -> assertThat(token.token()).isEqualTo(REFRESHED_JWT));

        // Verify refresh token was used (not password)
        server.verify(postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(matchingRefreshTokenAuth(INITIAL_REFRESH_TOKEN)));
    }

    @Test
    void refreshTokenRotation() {
        // Given
        stubPasswordAuth(INITIAL_JWT, INITIAL_REFRESH_TOKEN);
        stubRefreshTokenAuth(INITIAL_REFRESH_TOKEN, REFRESHED_JWT, NEW_REFRESH_TOKEN);
        stubRefreshTokenAuth(NEW_REFRESH_TOKEN, "third-jwt", "third-refresh-token");

        // First call: password auth
        assertThat(service.getBearerToken())
                .succeedsWithin(Duration.ofSeconds(5));

        // Second call: use initial refresh token, get new refresh token
        assertThat(service.getBearerToken())
                .succeedsWithin(Duration.ofSeconds(5));
        server.resetRequests();

        // When
        // Third call: should use NEW refresh token (rotation)
        var thirdTokenStage = service.getBearerToken();

        // Then
        assertThat(thirdTokenStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .satisfies(token -> assertThat(token.token()).isEqualTo("third-jwt"));

        // Verify the NEW refresh token was used (proving rotation occurred)
        server.verify(postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(matchingRefreshTokenAuth(NEW_REFRESH_TOKEN)));
    }

    @Test
    void refreshTokenFailureFallsBackToPasswordAuth() {
        // Given
        stubPasswordAuth(INITIAL_JWT, INITIAL_REFRESH_TOKEN);

        // First call: password auth captures refresh token
        service.getBearerToken().toCompletableFuture().join();

        // Stub refresh token to fail, but password auth to succeed
        stubRefreshTokenFailure(INITIAL_REFRESH_TOKEN);
        stubPasswordAuth(REFRESHED_JWT, NEW_REFRESH_TOKEN);

        server.resetRequests();

        // When
        // Second call: refresh token fails, should fall back to password
        var secondTokenStage = service.getBearerToken();

        // Then
        assertThat(secondTokenStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .satisfies(token -> assertThat(token.token())
                        .isEqualTo(REFRESHED_JWT));

        // Verify both refresh attempt AND password fallback occurred
        server.verify(1, postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(matchingRefreshTokenAuth(INITIAL_REFRESH_TOKEN)));
        server.verify(1, postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(matchingPasswordAuth()));
    }

    @Test
    void domainIsSentInPasswordAuthRequest() {
        // Given
        stubPasswordAuthWithDomain("my-domain", INITIAL_JWT, INITIAL_REFRESH_TOKEN);
        service = buildService("my-domain");

        // When
        var tokenStage = service.getBearerToken();

        // Then
        assertThat(tokenStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .satisfies(token -> assertThat(token.token()).isEqualTo(INITIAL_JWT));

        server.verify(postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(containing("\"domain\":\"my-domain\"")));
    }

    @Test
    void domainIsAbsentFromPasswordAuthRequestWhenNotConfigured() {
        // Given
        stubPasswordAuth(INITIAL_JWT, INITIAL_REFRESH_TOKEN);

        // When
        var tokenStage = service.getBearerToken();

        // Then
        assertThat(tokenStage).succeedsWithin(Duration.ofSeconds(5));

        server.verify(postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(containing("\"grant_type\":\"password\""))
                .withRequestBody(not(containing("\"domain\""))));
    }

    @Test
    void domainIsAbsentFromRefreshTokenRequest() {
        // Given
        stubPasswordAuthWithDomain("my-domain", INITIAL_JWT, INITIAL_REFRESH_TOKEN);
        stubRefreshTokenAuth(INITIAL_REFRESH_TOKEN, REFRESHED_JWT, NEW_REFRESH_TOKEN);
        service = buildService("my-domain");

        // First call: password auth with domain
        assertThat(service.getBearerToken()).succeedsWithin(Duration.ofSeconds(5));
        server.resetRequests();

        // When: second call uses refresh token
        assertThat(service.getBearerToken()).succeedsWithin(Duration.ofSeconds(5));

        // Then: refresh request must not contain domain
        server.verify(postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(containing("\"grant_type\":\"refresh_token\""))
                .withRequestBody(not(containing("\"domain\""))));
    }

    @Test
    void authenticationFailsWithInvalidCredentials() {
        // Given - stub authentication failure
        stubPasswordAuthFailure();

        // When
        var tokenStage = service.getBearerToken();

        // Then
        assertThat(tokenStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .havingRootCause()
                .withMessageContaining("password authentication failed with HTTP 401");
    }

    // Helper methods for stubbing

    private void stubPasswordAuth(String jwt, String refreshToken) {
        String response = """
                {
                    "jwt": "%s",
                    "duration": 300,
                    "refresh_token": "%s"
                }
                """.formatted(jwt, refreshToken);

        server.stubFor(
                post(urlPathEqualTo("/api/v1/auth/tokens/"))
                        .withRequestBody(matchingPasswordAuth())
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));
    }

    private void stubPasswordAuthWithDomain(String domain, String jwt, String refreshToken) {
        String response = """
                {
                    "jwt": "%s",
                    "duration": 300,
                    "refresh_token": "%s"
                }
                """.formatted(jwt, refreshToken);

        server.stubFor(
                post(urlPathEqualTo("/api/v1/auth/tokens/"))
                        .withRequestBody(matchingPasswordAuth().and(containing("\"domain\":\"" + domain + "\"")))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));
    }

    private void stubRefreshTokenAuth(String refreshToken, String newJwt, String newRefreshToken) {
        // Note: Real CipherTrust Manager returns the SAME refresh token (no rotation)
        // but we test with new tokens to verify the storage mechanism works
        String response = """
                {
                    "jwt": "%s",
                    "duration": 300,
                    "refresh_token": "%s"
                }
                """.formatted(newJwt, newRefreshToken);

        server.stubFor(
                post(urlPathEqualTo("/api/v1/auth/tokens/"))
                        .withRequestBody(matchingRefreshTokenAuth(refreshToken))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));
    }

    private void stubPasswordAuthFailure() {
        String errorResponse = """
                {
                    "code": 5,
                    "codeDesc": "NCERRUnauthorizedAccess",
                    "message": "Wrong username or password.",
                    "requestID": "test-request-id"
                }
                """;

        server.stubFor(
                post(urlPathEqualTo("/api/v1/auth/tokens/"))
                        .withRequestBody(matchingPasswordAuth())
                        .willReturn(aResponse()
                                .withStatus(401)
                                .withHeader("Content-Type", "application/json")
                                .withBody(errorResponse)));
    }

    private void stubRefreshTokenFailure(String refreshToken) {
        String errorResponse = """
                {
                    "code": 5,
                    "codeDesc": "NCERRUnauthorizedAccess",
                    "message": "Invalid token",
                    "requestID": "test-request-id"
                }
                """;

        server.stubFor(
                post(urlPathEqualTo("/api/v1/auth/tokens/"))
                        .withRequestBody(matchingRefreshTokenAuth(refreshToken))
                        .willReturn(aResponse()
                                .withStatus(401)
                                .withHeader("Content-Type", "application/json")
                                .withBody(errorResponse)));
    }

    // Helper methods for request matching

    private StringValuePattern matchingPasswordAuth() {
        return containing("\"grant_type\":\"password\"");
    }

    private StringValuePattern matchingRefreshTokenAuth(String refreshToken) {
        return containing("\"grant_type\":\"refresh_token\"")
                .and(containing("\"refresh_token\":\"" + refreshToken + "\""));
    }
}
