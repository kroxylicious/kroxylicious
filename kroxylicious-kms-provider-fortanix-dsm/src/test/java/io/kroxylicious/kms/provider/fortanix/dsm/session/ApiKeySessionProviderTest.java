/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.session;

import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;

import io.kroxylicious.kms.provider.fortanix.dsm.config.ApiKeySessionProviderConfig;
import io.kroxylicious.kms.provider.fortanix.dsm.config.Config;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.kroxylicious.kms.provider.fortanix.dsm.FortanixDsmKms.AUTHORIZATION_HEADER;
import static io.kroxylicious.kms.provider.fortanix.dsm.session.ApiKeySessionProvider.SESSION_TERMINATE_ENDPOINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ApiKeySessionProviderTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String SESSION_AUTH_ENDPOINT = "/sys/v1/session/auth";
    private static final String SESSION_AUTH_RESPONSE = """
            {"token_type":"Bearer",
             "expires_in":600,
             "access_token":"4oHsBCiTBMHvqquNf0fowkdgUheKHvd10uiw-950QLRkOUw1TC5yNfxezg3hDMFCyGAeuojh-u14AUcujcDuIQ",
             "entity_id":"f1bf09fa-a99b-4532-934c-0cb4eee427a4",
             "allowed_mfa_methods":[]}
            """; // notsecret

    private static final String SESSION_AUTH_RESPONSE_WITH_ADDITIONAL_PROPERTIES = """
            {"token_type":"Bearer",
             "expires_in":600,
             "access_token":"4oHsBCiTBMHvqquNf0fowkdgUheKHvd10uiw-950QLRkOUw1TC5yNfxezg3hDMFCyGAeuojh-u14AUcujcDuIQ",
             "entity_id":"f1bf09fa-a99b-4532-934c-0cb4eee427a4",
             "foo":"bar"}
            """; // notsecret

    private static WireMockServer server;
    private Config config;
    private HttpClient client;

    @BeforeAll
    public static void initMockRegistry() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
    }

    @AfterAll
    public static void shutdownMockRegistry() {
        server.shutdown();
    }

    @BeforeEach
    void setUp() {
        config = new Config(URI.create(server.baseUrl()), new ApiKeySessionProviderConfig(new InlinePassword("apiKey"), 0.20), null);
        client = HttpClient.newBuilder().build();

        server.stubFor(
                post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                        .withHeader(AUTHORIZATION_HEADER, containing("Basic "))
                        .willReturn(aResponse().withBody(SESSION_AUTH_RESPONSE)));

        server.stubFor(
                post(urlEqualTo(SESSION_TERMINATE_ENDPOINT))
                        .willReturn(aResponse().withStatus(204)));
    }

    @AfterEach
    void afterEach() {
        server.resetAll();
    }

    @Test
    void rejectsNullConfig() {
        assertThatThrownBy(() -> new ApiKeySessionProvider(null, client))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void sessionEstablishedSuccessfully() {
        var fixedClock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

        try (var provider = new ApiKeySessionProvider(config, client, fixedClock)) {
            var session = provider.getSession();
            assertThat(session)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .satisfies(s -> {
                        assertThat(s.authorizationHeader()).isEqualTo("Bearer 4oHsBCiTBMHvqquNf0fowkdgUheKHvd10uiw-950QLRkOUw1TC5yNfxezg3hDMFCyGAeuojh-u14AUcujcDuIQ");
                        assertThat(s.expiration()).isEqualTo(Instant.parse("1970-01-01T00:10:00Z"));
                    });
        }
    }

    @Test
    void ignoresAdditionalPropertiesInResponse() {
        server.stubFor(
                post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                        .willReturn(aResponse().withBody(SESSION_AUTH_RESPONSE_WITH_ADDITIONAL_PROPERTIES)));

        try (var provider = new ApiKeySessionProvider(config, client)) {
            var session = provider.getSession();
            assertThat(session)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .satisfies(s -> assertThat(s.authorizationHeader())
                            .isEqualTo("Bearer 4oHsBCiTBMHvqquNf0fowkdgUheKHvd10uiw-950QLRkOUw1TC5yNfxezg3hDMFCyGAeuojh-u14AUcujcDuIQ"));
        }
    }

    @Test
    void subsequentCallReturnsCachedSession() {
        var now = Instant.now();
        var fixedClock = Clock.fixed(now, ZoneId.systemDefault());

        try (var provider = new ApiKeySessionProvider(config, client, fixedClock)) {
            var sessionStage = provider.getSession();
            assertThat(sessionStage)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .isNotNull();

            server.resetAll();

            var session = sessionStage.toCompletableFuture().join();

            var again = provider.getSession();
            assertThat(again)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .isSameAs(session);
        }
    }

    /**
     * This test ensures that the session gets refreshed, preemptively, before its expiration time.
     */
    @Test
    void sessionGetsPreemptivelyRefreshed() {
        var now = Instant.now();
        var expiresInSecs = 10;

        var initial = createTestCredential("Bearer", expiresInSecs, "firstToken");

        server.stubFor(
                post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                        .willReturn(aResponse()
                                .withBody(toJson(initial))));

        try (var provider = new ApiKeySessionProvider(config, client, Clock.fixed(now, ZoneId.systemDefault()))) {
            var sessionStage = provider.getSession();
            assertThat(sessionStage)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .satisfies(s -> assertThat(s.authorizationHeader()).isEqualTo("Bearer firstToken"));

            var refreshed = createTestCredential("Bearer", expiresInSecs, "secondToken");
            server.stubFor(
                    post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                            .willReturn(aResponse().withBody(toJson(refreshed))));

            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> {
                        var refreshedStage = provider.getSession();
                        assertThat(refreshedStage)
                                .succeedsWithin(Duration.ofSeconds(1))
                                .satisfies(rs -> assertThat(rs.authorizationHeader()).isEqualTo("Bearer secondToken"));
                    });
        }
    }

    /**
     * This test ensures that previous sessions actually get terminated on the server.
     */
    @Test
    void previousSessionTerminatedOnServer() {
        var now = Instant.now();
        var expiresInSecs = 10;

        var initialToken = "firstToken-" + UUID.randomUUID();
        var secondToken = "secondToken-" + UUID.randomUUID();

        var initial = createTestCredential("Bearer", expiresInSecs, initialToken);

        server.stubFor(
                post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                        .willReturn(aResponse()
                                .withBody(toJson(initial))));

        try (var provider = new ApiKeySessionProvider(config, client, Clock.fixed(now, ZoneId.systemDefault()))) {
            var sessionStage = provider.getSession();
            assertThat(sessionStage)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .satisfies(s -> assertThat(s.authorizationHeader()).isEqualTo("Bearer " + initialToken));

            var refreshed = createTestCredential("Bearer", expiresInSecs, secondToken);
            server.stubFor(
                    post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                            .willReturn(aResponse().withBody(toJson(refreshed))));

            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> {
                        server.verify(1, postRequestedFor(urlEqualTo(SESSION_TERMINATE_ENDPOINT))
                                .withHeader(AUTHORIZATION_HEADER, equalTo("Bearer " + initialToken)));
                    });
        }
    }

    @Test
    void shouldTerminateSession() {
        // Given
        var now = Instant.now();
        var expiresInSecs = 10;
        var initialToken = "firstToken-" + UUID.randomUUID();
        var sessionAuthResponse = createTestCredential("Bearer", expiresInSecs, initialToken);

        server.stubFor(
                post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                        .willReturn(aResponse()
                                .withBody(toJson(sessionAuthResponse))));

        try (var provider = new ApiKeySessionProvider(config, client, Clock.fixed(now, ZoneId.systemDefault()))) {
            // When
            provider.terminateSessionOnServer(provider.getSession());
            // Then
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> {
                        server.verify(1, postRequestedFor(urlEqualTo(SESSION_TERMINATE_ENDPOINT))
                                .withHeader(AUTHORIZATION_HEADER, equalTo("Bearer " + initialToken)));
                    });
        }
    }

    /**
     * This test ensures if a session somehow expires (because time is beyond its expiration)
     * that it get refreshed anyway.
     */
    @Test
    void expiredSessionRefreshed() {
        var factorSoLargePreemptiveRefreshWillBeAfterExpiry = 2.0;
        var expiresInSecs = 10;
        var cfg = new ApiKeySessionProviderConfig(new InlinePassword("apiKey"), factorSoLargePreemptiveRefreshWillBeAfterExpiry);
        var now = Instant.now();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var initial = createTestCredential("Bearer", expiresInSecs, "firstToken");

        server.stubFor(
                post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                        .willReturn(aResponse().withBody(toJson(initial))));

        try (var provider = new ApiKeySessionProvider(new Config(URI.create(server.baseUrl()), cfg, null), client, clock)) {
            var sessionStage = provider.getSession();
            assertThat(sessionStage)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .satisfies(s -> assertThat(s.authorizationHeader()).isEqualTo("Bearer firstToken"));

            var refreshed = createTestCredential("Bearer", expiresInSecs, "secondToken");
            server.stubFor(
                    post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                            .willReturn(aResponse()
                                    .withBody(toJson(refreshed))));

            // advance time so that the initial token has past its expiration.
            var timeBeyondInitialExpiry = now.plusSeconds(expiresInSecs + 1);
            when(clock.instant()).thenReturn(timeBeyondInitialExpiry);

            sessionStage = provider.getSession();
            assertThat(sessionStage)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .satisfies(s -> assertThat(s.authorizationHeader()).isEqualTo("Bearer secondToken"));
        }
    }

    /**
     * This test ensures if a session is invalidated, the next call gets a new session.
     */
    @Test
    void invalidatedSessionGetsRefreshed() {
        var factorSoLargePreemptiveRefreshWillBeAfterExpiry = 2.0;
        var expiresInSecs = 10;
        var cfg = new ApiKeySessionProviderConfig(new InlinePassword("apiKey"), factorSoLargePreemptiveRefreshWillBeAfterExpiry);

        var initial = createTestCredential("Bearer", expiresInSecs, "firstToken");

        server.stubFor(
                post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                        .willReturn(aResponse().withBody(toJson(initial))));

        try (var provider = new ApiKeySessionProvider(new Config(URI.create(server.baseUrl()), cfg, null), client)) {
            var sessionStage = provider.getSession();
            assertThat(sessionStage)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .satisfies(s -> assertThat(s.authorizationHeader()).isEqualTo("Bearer firstToken"));

            var session = sessionStage.toCompletableFuture().join();
            session.invalidate();

            var refreshed = createTestCredential("Bearer", expiresInSecs, "secondToken");
            server.stubFor(
                    post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                            .willReturn(aResponse().withBody(toJson(refreshed))));

            sessionStage = provider.getSession();
            assertThat(sessionStage)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .satisfies(s -> assertThat(s.authorizationHeader()).isEqualTo("Bearer secondToken"));
        }
    }

    @Test
    void sessionAuthRequestFails() {
        server.stubFor(
                post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                        .willReturn(aResponse().withStatus(500)));

        try (var provider = new ApiKeySessionProvider(config, client)) {
            var result = provider.getSession();
            assertThat(result)
                    .failsWithin(Duration.ofSeconds(1))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("HTTP status code 500");
        }
    }

    @Test
    void securityCredentialRetrievedAfterRequestFails() {
        server.stubFor(
                post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                        .willReturn(aResponse().withStatus(500)));

        try (var provider = new ApiKeySessionProvider(config, client)) {
            var result = provider.getSession();
            assertThat(result)
                    .failsWithin(Duration.ofSeconds(1))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("HTTP status code 500");

            var initial = createTestCredential("Bearer", 10, "firstToken");
            server.stubFor(
                    post(urlEqualTo(SESSION_AUTH_ENDPOINT))
                            .willReturn(aResponse()
                                    .withBody(toJson(initial))));

            result = provider.getSession();
            assertThat(result)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .satisfies(s -> assertThat(s.authorizationHeader()).isEqualTo("Bearer firstToken"));
        }
    }

    @Test
    @SuppressWarnings("java:S2699")
    void idempotentClose() {
        var provider = new ApiKeySessionProvider(config, client);
        provider.close();
        provider.close(); // should complete without error.
    }

    private SessionAuthResponse createTestCredential(String tokenType, int expiresIn, String accessToken) {
        return new SessionAuthResponse(tokenType, expiresIn, accessToken, "entityId", List.of());
    }

    private byte[] toJson(SessionAuthResponse authResponse) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(authResponse);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

}
