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

import io.kroxylicious.kms.provider.aws.kms.config.WebIdentityCredentialsProviderConfig;
import io.kroxylicious.kms.provider.aws.kms.credentials.WebIdentityCredentialsProvider.AssumedRoleCredentials;
import io.kroxylicious.kms.service.KmsException;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class WebIdentityCredentialsProviderTest {

    private static final String STS_PATH = "/";
    private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/KroxyliciousIRSA";
    private static final String DEFAULT_REGION = "us-east-1";

    private static WireMockServer stsServer;

    @TempDir
    Path tmp;

    private Path tokenFile;

    private static String emptyEnv(String name) {
        return null;
    }

    private final Function<String, String> emptyEnv = WebIdentityCredentialsProviderTest::emptyEnv;

    @BeforeAll
    static void initStsServer() {
        stsServer = new WireMockServer(wireMockConfig().dynamicPort());
        stsServer.start();
    }

    @AfterAll
    static void shutdownStsServer() {
        stsServer.shutdown();
    }

    @BeforeEach
    void setUp() throws IOException {
        tokenFile = tmp.resolve("token");
        Files.writeString(tokenFile, "header.payload.signature");
    }

    @AfterEach
    void afterEach() {
        stsServer.resetAll();
    }

    @Test
    void rejectsMissingRoleArn() {
        var cfg = config(null, tokenFile, null, null);
        assertThatThrownBy(() -> new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, emptyEnv, Clock.systemUTC()))
                .isInstanceOf(KmsException.class)
                .hasMessageContaining("roleArn");
    }

    @Test
    void rejectsMissingTokenFile() {
        var cfg = config(ROLE_ARN, null, null, null);
        assertThatThrownBy(() -> new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, emptyEnv, Clock.systemUTC()))
                .isInstanceOf(KmsException.class)
                .hasMessageContaining("webIdentityTokenFile");
    }

    @Test
    void resolvesRoleArnFromEnv() {
        var cfg = config(null, tokenFile, null, null);
        var env = envOf(Map.of(WebIdentityCredentialsProvider.ENV_ROLE_ARN, ROLE_ARN));
        try (var provider = new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, env, Clock.systemUTC())) {
            assertThat(provider.roleArn()).isEqualTo(ROLE_ARN);
        }
    }

    @Test
    void resolvesTokenFileFromEnv() {
        var cfg = config(ROLE_ARN, null, null, null);
        var env = envOf(Map.of(WebIdentityCredentialsProvider.ENV_WEB_IDENTITY_TOKEN_FILE, tokenFile.toString()));
        try (var provider = new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, env, Clock.systemUTC())) {
            assertThat(provider.roleArn()).isEqualTo(ROLE_ARN);
        }
    }

    @Test
    void defaultsStsEndpointToRegionalUrl() {
        var cfg = config(ROLE_ARN, tokenFile, null, null);
        try (var provider = new WebIdentityCredentialsProvider(cfg, "eu-west-2", emptyEnv, Clock.systemUTC())) {
            assertThat(provider.stsEndpointUrl()).isEqualTo(URI.create("https://sts.eu-west-2.amazonaws.com"));
        }
    }

    @Test
    void stsRegionEnvOverridesDefault() {
        var cfg = config(ROLE_ARN, tokenFile, null, null);
        var env = envOf(Map.of(WebIdentityCredentialsProvider.ENV_AWS_REGION, "ap-southeast-1"));
        try (var provider = new WebIdentityCredentialsProvider(cfg, "us-east-1", env, Clock.systemUTC())) {
            assertThat(provider.stsEndpointUrl()).isEqualTo(URI.create("https://sts.ap-southeast-1.amazonaws.com"));
        }
    }

    @Test
    void generatesSessionNameWhenAbsent() {
        var cfg = config(ROLE_ARN, tokenFile, null, null);
        try (var provider = new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, emptyEnv, Clock.systemUTC())) {
            assertThat(provider.roleSessionName()).startsWith("kroxylicious-").hasSizeLessThanOrEqualTo(64);
        }
    }

    @Test
    void sanitisesIllegalCharsInSessionName() {
        var cfg = config(ROLE_ARN, tokenFile, "bad name!", null);
        try (var provider = new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, emptyEnv, Clock.systemUTC())) {
            assertThat(provider.roleSessionName()).matches("[\\w+=,.@-]+").doesNotContain(" ", "!");
        }
    }

    @Test
    void retrievesCredentialsFromSts() {
        stubStsSuccess("ASIATESTKEY", "secretValue", "sessionTokenValue", Instant.parse("2099-01-01T00:00:00Z"));

        var cfg = config(ROLE_ARN, tokenFile, "my-session", URI.create(stsServer.baseUrl() + STS_PATH));
        try (var provider = new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, emptyEnv, Clock.systemUTC())) {
            var stage = provider.getCredentials();
            assertThat(stage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .returns("ASIATESTKEY", AssumedRoleCredentials::accessKeyId)
                    .returns("secretValue", AssumedRoleCredentials::secretAccessKey)
                    .returns("sessionTokenValue", AssumedRoleCredentials::sessionToken)
                    .returns(Instant.parse("2099-01-01T00:00:00Z"), AssumedRoleCredentials::expiration);
        }

        stsServer.verify(WireMock.postRequestedFor(urlEqualTo(STS_PATH))
                .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
                .withHeader("Accept", equalTo("application/json"))
                .withRequestBody(containing("Action=AssumeRoleWithWebIdentity"))
                .withRequestBody(containing("Version=2011-06-15"))
                .withRequestBody(containing("RoleSessionName=my-session"))
                .withRequestBody(containing("WebIdentityToken=header.payload.signature"))
                .withRequestBody(containing("RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2FKroxyliciousIRSA")));
    }

    @Test
    void readsTokenFileFreshOnRefresh() throws IOException {
        var initialExp = Instant.now().plusSeconds(2);
        var refreshedExp = Instant.now().plusSeconds(20);

        // First response uses the original token file
        stsServer.stubFor(post(urlEqualTo(STS_PATH))
                .inScenario("rotation").whenScenarioStateIs("Started")
                .withRequestBody(containing("WebIdentityToken=header.payload.signature"))
                .willReturn(aResponse().withBody(stsBody("ASIA1", "s1", "t1", initialExp)))
                .willSetStateTo("rotated"));

        // After rotation only the new token bytes should be accepted
        stsServer.stubFor(post(urlEqualTo(STS_PATH))
                .inScenario("rotation").whenScenarioStateIs("rotated")
                .withRequestBody(containing("WebIdentityToken=ROTATED.PAYLOAD.SIG"))
                .willReturn(aResponse().withBody(stsBody("ASIA2", "s2", "t2", refreshedExp))));

        var cfg = config(ROLE_ARN, tokenFile, "session", URI.create(stsServer.baseUrl() + STS_PATH));
        try (var provider = new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, emptyEnv, Clock.systemUTC())) {
            var stage = provider.getCredentials();
            assertThat(stage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .returns("ASIA1", AssumedRoleCredentials::accessKeyId);

            // Rotate the projected token on disk before the refresh fires
            Files.writeString(tokenFile, "ROTATED.PAYLOAD.SIG");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                var refreshed = provider.getCredentials();
                assertThat(refreshed)
                        .succeedsWithin(Duration.ofSeconds(1))
                        .returns("ASIA2", AssumedRoleCredentials::accessKeyId);
            });
        }
    }

    @Test
    void surfacesStsErrorMessage() {
        var errorBody = """
                {
                  "ErrorResponse": {
                    "Error": {
                      "Type": "Sender",
                      "Code": "InvalidIdentityToken",
                      "Message": "Couldn't retrieve verification key"
                    },
                    "RequestId": "abc-123"
                  }
                }
                """;
        stsServer.stubFor(post(urlEqualTo(STS_PATH))
                .willReturn(aResponse().withStatus(400).withBody(errorBody)));

        var cfg = config(ROLE_ARN, tokenFile, "s", URI.create(stsServer.baseUrl() + STS_PATH));
        try (var provider = new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, emptyEnv, Clock.systemUTC())) {
            var stage = provider.getCredentials();
            assertThat(stage)
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("InvalidIdentityToken")
                    .withMessageContaining("Couldn't retrieve verification key");
        }
    }

    @Test
    void surfacesNonJsonStsError() {
        stsServer.stubFor(post(urlEqualTo(STS_PATH))
                .willReturn(aResponse().withStatus(500).withBody("upstream boom")));

        var cfg = config(ROLE_ARN, tokenFile, "s", URI.create(stsServer.baseUrl() + STS_PATH));
        try (var provider = new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, emptyEnv, Clock.systemUTC())) {
            assertThat(provider.getCredentials())
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("HTTP 500")
                    .withMessageContaining("upstream boom");
        }
    }

    @Test
    void rejectsMissingTokenFileOnRefresh() throws IOException {
        Files.delete(tokenFile);
        var cfg = config(ROLE_ARN, tokenFile, "s", URI.create(stsServer.baseUrl() + STS_PATH));
        try (var provider = new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, emptyEnv, Clock.systemUTC())) {
            assertThat(provider.getCredentials())
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .havingRootCause()
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void includesDurationSecondsWhenSet() {
        stubStsSuccess("ASIA", "s", "t", Instant.parse("2099-01-01T00:00:00Z"));

        var cfg = new WebIdentityCredentialsProviderConfig(ROLE_ARN, tokenFile, "s",
                URI.create(stsServer.baseUrl() + STS_PATH), null, 1800, null);
        try (var provider = new WebIdentityCredentialsProvider(cfg, DEFAULT_REGION, emptyEnv, Clock.systemUTC())) {
            assertThat(provider.getCredentials()).succeedsWithin(Duration.ofSeconds(5));
        }

        stsServer.verify(WireMock.postRequestedFor(urlEqualTo(STS_PATH))
                .withRequestBody(containing("DurationSeconds=1800")));
    }

    private WebIdentityCredentialsProviderConfig config(String roleArn, Path tokenFile, String sessionName, URI stsEndpoint) {
        return new WebIdentityCredentialsProviderConfig(roleArn, tokenFile, sessionName, stsEndpoint, null, null, null);
    }

    private static Function<String, String> envOf(Map<String, String> values) {
        var copy = new HashMap<>(values);
        return copy::get;
    }

    private void stubStsSuccess(String accessKey, String secret, String token, Instant expiration) {
        stsServer.stubFor(post(urlEqualTo(STS_PATH))
                .willReturn(aResponse().withBody(stsBody(accessKey, secret, token, expiration))));
    }

    private static byte[] stsBody(String accessKey, String secret, String token, Instant expiration) {
        var json = """
                {
                  "AssumeRoleWithWebIdentityResponse": {
                    "AssumeRoleWithWebIdentityResult": {
                      "Credentials": {
                        "AccessKeyId": "%s",
                        "SecretAccessKey": "%s",
                        "SessionToken": "%s",
                        "Expiration": "%s"
                      }
                    }
                  }
                }
                """.formatted(accessKey, secret, token, expiration);
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
