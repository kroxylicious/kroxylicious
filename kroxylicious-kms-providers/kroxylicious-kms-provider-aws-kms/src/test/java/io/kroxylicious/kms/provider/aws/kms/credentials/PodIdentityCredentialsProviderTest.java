/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;

import io.kroxylicious.kms.provider.aws.kms.config.PodIdentityCredentialsProviderConfig;
import io.kroxylicious.kms.provider.aws.kms.credentials.PodIdentityCredentialsProvider.PodIdentityCredentials;
import io.kroxylicious.kms.service.KmsException;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class PodIdentityCredentialsProviderTest {

    private static final String CREDENTIALS_PATH = "/v1/credentials";

    private static WireMockServer agentServer;

    @TempDir
    Path tmp;

    private Path tokenFile;
    private URI credentialsUri;

    private static String emptyEnv(String name) {
        return null;
    }

    private final Function<String, String> emptyEnv = PodIdentityCredentialsProviderTest::emptyEnv;

    @BeforeAll
    static void initAgent() {
        agentServer = new WireMockServer(wireMockConfig().dynamicPort());
        agentServer.start();
    }

    @AfterAll
    static void shutdownAgent() {
        agentServer.shutdown();
    }

    @BeforeEach
    void setUp() throws IOException {
        tokenFile = tmp.resolve("token");
        Files.writeString(tokenFile, "bearer-token-value");
        credentialsUri = URI.create(agentServer.baseUrl() + CREDENTIALS_PATH);
    }

    @AfterEach
    void afterEach() {
        agentServer.resetAll();
    }

    @Test
    void rejectsMissingUri() {
        var cfg = config(null, tokenFile);
        assertThatThrownBy(() -> new PodIdentityCredentialsProvider(cfg, emptyEnv, Clock.systemUTC()))
                .isInstanceOf(KmsException.class)
                .hasMessageContaining("credentialsFullUri");
    }

    @Test
    void rejectsMissingTokenFile() {
        var cfg = config(credentialsUri, null);
        assertThatThrownBy(() -> new PodIdentityCredentialsProvider(cfg, emptyEnv, Clock.systemUTC()))
                .isInstanceOf(KmsException.class)
                .hasMessageContaining("authorizationTokenFile");
    }

    @Test
    void rejectsNonHttpScheme() {
        var cfg = config(URI.create("file:///etc/passwd"), tokenFile);
        assertThatThrownBy(() -> new PodIdentityCredentialsProvider(cfg, emptyEnv, Clock.systemUTC()))
                .isInstanceOf(KmsException.class)
                .hasMessageContaining("http or https");
    }

    @Test
    void resolvesUriFromEnv() {
        var cfg = config(null, tokenFile);
        var env = envOf(Map.of(PodIdentityCredentialsProvider.ENV_FULL_URI, credentialsUri.toString()));
        try (var provider = new PodIdentityCredentialsProvider(cfg, env, Clock.systemUTC())) {
            assertThat(provider.credentialsFullUri()).isEqualTo(credentialsUri);
        }
    }

    @Test
    void resolvesTokenFileFromEnv() {
        var cfg = config(credentialsUri, null);
        var env = envOf(Map.of(PodIdentityCredentialsProvider.ENV_AUTH_TOKEN_FILE, tokenFile.toString()));
        try (var provider = new PodIdentityCredentialsProvider(cfg, env, Clock.systemUTC())) {
            assertThat(provider.authorizationTokenFile()).isEqualTo(tokenFile);
        }
    }

    @Test
    void retrievesCredentialsFromAgent() {
        stubAgentSuccess("ASIATESTKEY", "secretValue", "tokenValue", Instant.parse("2099-01-01T00:00:00Z"));

        var cfg = config(credentialsUri, tokenFile);
        try (var provider = new PodIdentityCredentialsProvider(cfg, emptyEnv, Clock.systemUTC())) {
            var stage = provider.getCredentials();
            assertThat(stage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .returns("ASIATESTKEY", PodIdentityCredentials::accessKeyId)
                    .returns("secretValue", PodIdentityCredentials::secretAccessKey)
                    .returns("tokenValue", PodIdentityCredentials::token)
                    .returns(Instant.parse("2099-01-01T00:00:00Z"), PodIdentityCredentials::expiration);
        }

        agentServer.verify(WireMock.getRequestedFor(urlEqualTo(CREDENTIALS_PATH))
                .withHeader("Authorization", equalTo("bearer-token-value"))
                .withHeader("Accept", equalTo("application/json")));
    }

    @Test
    void readsTokenFileFreshOnRefresh() throws IOException {
        var initialExp = Instant.now().plusSeconds(2);
        var refreshedExp = Instant.now().plusSeconds(20);

        agentServer.stubFor(get(urlEqualTo(CREDENTIALS_PATH))
                .inScenario("rotation").whenScenarioStateIs("Started")
                .withHeader("Authorization", equalTo("bearer-token-value"))
                .willReturn(aResponse().withBody(agentBody("ASIA1", "s1", "t1", initialExp)))
                .willSetStateTo("rotated"));

        agentServer.stubFor(get(urlEqualTo(CREDENTIALS_PATH))
                .inScenario("rotation").whenScenarioStateIs("rotated")
                .withHeader("Authorization", equalTo("ROTATED-TOKEN"))
                .willReturn(aResponse().withBody(agentBody("ASIA2", "s2", "t2", refreshedExp))));

        var cfg = config(credentialsUri, tokenFile);
        try (var provider = new PodIdentityCredentialsProvider(cfg, emptyEnv, Clock.systemUTC())) {
            assertThat(provider.getCredentials())
                    .succeedsWithin(Duration.ofSeconds(5))
                    .returns("ASIA1", PodIdentityCredentials::accessKeyId);

            Files.writeString(tokenFile, "ROTATED-TOKEN");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(provider.getCredentials())
                        .succeedsWithin(Duration.ofSeconds(1))
                        .returns("ASIA2", PodIdentityCredentials::accessKeyId);
            });
        }
    }

    @Test
    void surfacesNon2xxAsKmsException() {
        agentServer.stubFor(get(urlEqualTo(CREDENTIALS_PATH))
                .willReturn(aResponse().withStatus(500).withBody("agent down")));

        var cfg = config(credentialsUri, tokenFile);
        try (var provider = new PodIdentityCredentialsProvider(cfg, emptyEnv, Clock.systemUTC())) {
            assertThat(provider.getCredentials())
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("HTTP 500")
                    .withMessageContaining("agent down");
        }
    }

    @Test
    void rejectsMissingTokenFileOnRefresh() throws IOException {
        Files.delete(tokenFile);
        var cfg = config(credentialsUri, tokenFile);
        try (var provider = new PodIdentityCredentialsProvider(cfg, emptyEnv, Clock.systemUTC())) {
            assertThat(provider.getCredentials())
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .havingRootCause()
                    .isInstanceOf(IOException.class);
        }
    }

    private PodIdentityCredentialsProviderConfig config(URI uri, Path tokenFile) {
        return new PodIdentityCredentialsProviderConfig(uri, tokenFile, null);
    }

    private static Function<String, String> envOf(Map<String, String> values) {
        var copy = new HashMap<>(values);
        return copy::get;
    }

    private void stubAgentSuccess(String accessKey, String secret, String token, Instant expiration) {
        agentServer.stubFor(get(urlEqualTo(CREDENTIALS_PATH))
                .willReturn(aResponse().withBody(agentBody(accessKey, secret, token, expiration))));
    }

    private static byte[] agentBody(String accessKey, String secret, String token, Instant expiration) {
        var json = """
                {
                  "AccessKeyId": "%s",
                  "SecretAccessKey": "%s",
                  "Token": "%s",
                  "AccountId": "123456789012",
                  "Expiration": "%s"
                }
                """.formatted(accessKey, secret, token, expiration);
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
