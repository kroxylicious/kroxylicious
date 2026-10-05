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
        new KubernetesTokenProvider(httpClient, URI.create("http://vault:8200"), "ns", "role", "path", "kubernetes");
        new KubernetesTokenProvider(httpClient, URI.create("http://vault:8200/"), "ns/", "role", "path", "kubernetes/");
        new KubernetesTokenProvider(httpClient, URI.create("http://vault:8200"), "", "role", "path", "kubernetes/");
    }

    @Test
    void testGetTokenConcurrentRequests(@TempDir Path tempDir) throws Exception {
        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, "my-jwt-token");
        wireMockServer.stubFor(post(urlEqualTo("/v1/auth/kubernetes/login"))
                .willReturn(aResponse().withFixedDelay(100).withStatus(200).withBody("{\"auth\":{\"client_token\":\"tok\",\"lease_duration\":3600}}")));
        KubernetesTokenProvider provider = new KubernetesTokenProvider(httpClient, URI.create(wireMockServer.baseUrl()), null, "my-role", tokenFile.toString(),
                "kubernetes");
        assertThat(provider.getToken()).isSameAs(provider.getToken());
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

        CompletableFuture<String> secondCall = provider.getToken().toCompletableFuture();
        assertThat(secondCall).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("vault-client-token-2");
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
        wireMockServer.stubFor(post(urlEqualTo("/v1/my-ns/auth/kubernetes/login"))
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
}
