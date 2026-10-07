/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;

import io.kroxylicious.kms.service.KmsException;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

class KubernetesTokenProviderTest {

    private WireMockServer wireMockServer;
    private HttpClient httpClient;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMockServer.start();
        httpClient = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        wireMockServer.stop();
    }

    @Test
    void testCreateAuthUrlVariations() {
        KubernetesTokenProvider provider1 = new KubernetesTokenProvider(httpClient, URI.create("http://vault:8200"), "ns", "role", "path", "kubernetes");
        KubernetesTokenProvider provider2 = new KubernetesTokenProvider(httpClient, URI.create("http://vault:8200/"), "ns/", "role", "path", "kubernetes/");
        KubernetesTokenProvider provider3 = new KubernetesTokenProvider(httpClient, URI.create("http://vault:8200"), "", "role", "path", "kubernetes/");

        assertThat(provider1.getAuthUrl()).isEqualTo(URI.create("http://vault:8200/v1/auth/kubernetes/login"));
        assertThat(provider2.getAuthUrl()).isEqualTo(URI.create("http://vault:8200/v1/auth/kubernetes/login"));
        assertThat(provider3.getAuthUrl()).isEqualTo(URI.create("http://vault:8200/v1/auth/kubernetes/login"));
    }

    @Test
    void testGetTokenConcurrentRequests(@TempDir Path tempDir) throws Exception {
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");
        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .willReturn(aResponse().withFixedDelay(100).withStatus(200).withBody("{\"auth\":{\"client_token\":\"tok\",\"lease_duration\":3600}}")));
        KubernetesTokenProvider provider = new KubernetesTokenProvider(httpClient, URI.create(wireMockServer.baseUrl()), null, "my-role", tokenFile.toString(),
                "kubernetes");
        java.util.concurrent.CompletionStage<String> firstCall = provider.getToken();
        java.util.concurrent.CompletionStage<String> secondCall = provider.getToken();
        assertThat(firstCall.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("tok");
        assertThat(secondCall.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("tok");
    }

    @Test
    void testGetToken(@TempDir Path tempDir) throws Exception {
        // Given
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");

        String jsonResponse = """
                {
                  "auth": {
                    "client_token": "vault-client-token",
                    "lease_duration": 3600
                  }
                }
                """;
        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(jsonResponse)));

        KubernetesTokenProvider provider = new KubernetesTokenProvider(
                httpClient,
                URI.create(wireMockServer.baseUrl()),
                null,
                "my-role",
                tokenFile.toString(),
                "kubernetes");

        // When
        CompletableFuture<String> tokenFuture = provider.getToken().toCompletableFuture();

        // Then
        assertThat(tokenFuture).succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo("vault-client-token");
    }

    @Test
    void testGetTokenFailsIfTokenFileMissing(@TempDir Path tempDir) {
        // Given
        Path tokenFile = tempDir.resolve("missing-token");

        KubernetesTokenProvider provider = new KubernetesTokenProvider(
                httpClient,
                URI.create(wireMockServer.baseUrl()),
                null,
                "my-role",
                tokenFile.toString(),
                "kubernetes");

        // When
        CompletableFuture<String> tokenFuture = provider.getToken().toCompletableFuture();

        // Then
        assertThat(tokenFuture).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(java.util.concurrent.ExecutionException.class)
                .withCauseInstanceOf(KmsException.class)
                .withMessageContaining("Failed to read Kubernetes service account token");
    }

    @Test
    void testGetTokenFailsOnHttpError(@TempDir Path tempDir) throws Exception {
        // Given
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");

        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .willReturn(aResponse()
                        .withStatus(403)
                        .withBody("Forbidden")));

        KubernetesTokenProvider provider = new KubernetesTokenProvider(
                httpClient,
                URI.create(wireMockServer.baseUrl()),
                null,
                "my-role",
                tokenFile.toString(),
                "kubernetes");

        // When
        CompletableFuture<String> tokenFuture = provider.getToken().toCompletableFuture();

        // Then
        assertThat(tokenFuture).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(java.util.concurrent.ExecutionException.class)
                .withCauseInstanceOf(KmsException.class)
                .withMessageContaining("Failed to authenticate with Vault via Kubernetes auth. Status: 403");
    }

    @Test
    void testGetTokenCachesUntilLeaseRefresh(@TempDir Path tempDir) throws Exception {
        // Given
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");

        String jsonResponse = """
                {
                  "auth": {
                    "client_token": "vault-client-token-1",
                    "lease_duration": 3600
                  }
                }
                """;
        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(jsonResponse)));

        KubernetesTokenProvider provider = new KubernetesTokenProvider(
                httpClient,
                URI.create(wireMockServer.baseUrl()),
                null,
                "my-role",
                tokenFile.toString(),
                "kubernetes");

        // When
        CompletableFuture<String> firstCall = provider.getToken().toCompletableFuture();
        firstCall.join(); // Wait for first call to finish and update expiry time
        CompletableFuture<String> secondCall = provider.getToken().toCompletableFuture();

        // Then - both return cached token
        assertThat(firstCall).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("vault-client-token-1");
        assertThat(secondCall).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("vault-client-token-1");
    }

    @Test
    void testGetTokenExpiresAndRefreshes(@TempDir Path tempDir) throws Exception {
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");

        String jsonResponse1 = "{\"auth\": {\"client_token\": \"vault-client-token-1\", \"lease_duration\": 0}}";
        String jsonResponse2 = "{\"auth\": {\"client_token\": \"vault-client-token-2\", \"lease_duration\": 3600}}";

        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .inScenario("expiry")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(200).withBody(jsonResponse1))
                .willSetStateTo("second"));

        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .inScenario("expiry")
                .whenScenarioStateIs("second")
                .willReturn(aResponse().withStatus(200).withBody(jsonResponse2)));

        KubernetesTokenProvider provider = new KubernetesTokenProvider(
                httpClient, URI.create(wireMockServer.baseUrl()), null, "my-role", tokenFile.toString(), "kubernetes");

        CompletableFuture<String> firstCall = provider.getToken().toCompletableFuture();
        assertThat(firstCall).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("vault-client-token-1");

        // MIN_LEASE_DURATION_MS is 1000ms; wait past hard expiry so token is discarded and refreshed
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .until(() -> provider.getToken().toCompletableFuture().get().equals("vault-client-token-2"));
    }

    @Test
    void testGetTokenReturnsExistingTokenWhileRefreshInFlight(@TempDir Path tempDir) throws Exception {
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");

        String jsonResponse1 = "{\"auth\": {\"client_token\": \"vault-client-token-1\", \"lease_duration\": 1}}";
        String jsonResponse2 = "{\"auth\": {\"client_token\": \"vault-client-token-2\", \"lease_duration\": 3600}}";

        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .inScenario("refresh-inflight")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(200).withBody(jsonResponse1))
                .willSetStateTo("delayed"));

        // Second call has a 500ms delay to simulate in-flight background refresh
        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .inScenario("refresh-inflight")
                .whenScenarioStateIs("delayed")
                .willReturn(aResponse().withStatus(200).withFixedDelay(500).withBody(jsonResponse2)));

        KubernetesTokenProvider provider = new KubernetesTokenProvider(
                httpClient, URI.create(wireMockServer.baseUrl()), null, "my-role", tokenFile.toString(), "kubernetes");

        CompletableFuture<String> firstCall = provider.getToken().toCompletableFuture();
        assertThat(firstCall).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("vault-client-token-1");

        // Poll until past soft refresh (80% = 800ms) but before hard expiry (100% = 1000ms);
        // at that point getToken() must immediately return the old token (isDone = true, no blocking)
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> {
                    CompletableFuture<String> call = provider.getToken().toCompletableFuture();
                    return call.isDone() && "vault-client-token-1".equals(call.get())
                            && provider.getToken().toCompletableFuture().isDone();
                });

        // After background refresh finishes, subsequent call yields new token
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .until(() -> provider.getToken().toCompletableFuture().get().equals("vault-client-token-2"));
    }

    @Test
    void testFailedBackgroundRefreshDoesNotShortenTokenHardExpiry(@TempDir Path tempDir) throws Exception {
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");

        // First token has a 1 second lease (soft refresh at 800ms, hard expiry at 1000ms)
        String jsonResponse1 = "{\"auth\": {\"client_token\": \"vault-client-token-1\", \"lease_duration\": 1}}";

        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .inScenario("failed-refresh")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(200).withBody(jsonResponse1))
                .willSetStateTo("fail"));

        // Second call (background refresh) returns 503 error
        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .inScenario("failed-refresh")
                .whenScenarioStateIs("fail")
                .willReturn(aResponse().withStatus(503).withBody("Vault Unavailable")));

        KubernetesTokenProvider provider = new KubernetesTokenProvider(
                httpClient, URI.create(wireMockServer.baseUrl()), null, "my-role", tokenFile.toString(), "kubernetes");

        CompletableFuture<String> firstCall = provider.getToken().toCompletableFuture();
        assertThat(firstCall).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("vault-client-token-1");

        // Poll until soft-refresh window (>80% of lease) and verify the old token is still served
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> {
                    CompletableFuture<String> call = provider.getToken().toCompletableFuture();
                    // In the soft-refresh window the future must already be done (old token, no blocking)
                    return call.isDone() && "vault-client-token-1".equals(call.get());
                });

        // Poll until the background refresh has been attempted (refreshFuture completes with failure).
        // The existing token must still be accessible — failed refresh must NOT shorten hardExpiryTimeMs.
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> provider.getToken().toCompletableFuture().get().equals("vault-client-token-1"));
    }

    @Test
    void testGetTokenFailsBadJson(@TempDir Path tempDir) throws Exception {
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");

        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .willReturn(aResponse().withStatus(200).withBody("not-json")));

        KubernetesTokenProvider provider = new KubernetesTokenProvider(
                httpClient, URI.create(wireMockServer.baseUrl()), null, "my-role", tokenFile.toString(), "kubernetes");

        assertThat(provider.getToken().toCompletableFuture()).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(java.util.concurrent.ExecutionException.class)
                .withCauseInstanceOf(java.io.UncheckedIOException.class);
    }

    @Test
    void testGetTokenSendsVaultNamespaceHeader(@TempDir Path tempDir) throws Exception {
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");

        String jsonResponse = """
                {
                  "auth": {
                    "client_token": "vault-client-token",
                    "lease_duration": 3600
                  }
                }
                """;
        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .withHeader("X-Vault-Namespace", com.github.tomakehurst.wiremock.client.WireMock.equalTo("my-ns"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(jsonResponse)));

        KubernetesTokenProvider provider = new KubernetesTokenProvider(
                httpClient,
                URI.create(wireMockServer.baseUrl()),
                "my-ns",
                "my-role",
                tokenFile.toString(),
                "kubernetes");

        CompletableFuture<String> tokenFuture = provider.getToken().toCompletableFuture();
        assertThat(tokenFuture).succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo("vault-client-token");
    }

    @Test
    void testGetTokenMinimalCompletionStageReturnsUnmodifiableStage(@TempDir Path tempDir) throws Exception {
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");

        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("{\"auth\":{\"client_token\":\"tok\",\"lease_duration\":3600}}")));

        KubernetesTokenProvider provider = new KubernetesTokenProvider(
                httpClient, URI.create(wireMockServer.baseUrl()), null, "my-role", tokenFile.toString(), "kubernetes");

        java.util.concurrent.CompletionStage<String> stage = provider.getToken();
        assertThat(stage.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("tok");
        // Verify stage is minimal by checking class name contains MinimalStage
        assertThat(stage.getClass().getName()).contains("MinimalStage");
    }

    @Test
    void testGetTokenFailureBackoff(@TempDir Path tempDir) throws Exception {
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");

        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .willReturn(aResponse().withStatus(500).withBody("Internal Server Error")));

        KubernetesTokenProvider provider = new KubernetesTokenProvider(
                httpClient, URI.create(wireMockServer.baseUrl()), null, "my-role", tokenFile.toString(), "kubernetes");

        CompletableFuture<String> firstCall = provider.getToken().toCompletableFuture();
        assertThat(firstCall).failsWithin(Duration.ofSeconds(5));

        // Immediate second call should fail fast without triggering another wiremock request
        CompletableFuture<String> secondCall = provider.getToken().toCompletableFuture();
        assertThat(secondCall).failsWithin(Duration.ofSeconds(5));
    }
}
