/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;

import io.kroxylicious.kms.provider.aws.kms.config.Ec2MetadataCredentialsProviderConfig;
import io.kroxylicious.kms.provider.aws.kms.credentials.Ec2MetadataCredentialsProvider.SecurityCredentials;
import io.kroxylicious.kms.service.KmsException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Ec2MetadataCredentialsProviderTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private static final String MY_TOKEN = "mytoken";
    private static final String IAM_ROLE = "myrole";
    private static final String TOKEN_RETRIEVAL_ENDPOINT = "/latest/api/token";
    private static final String META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT = "/2024-04-11/meta-data/iam/security-credentials/";

    // From https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-metadata-security-credentials.html
    private static final String KNOWN_GOOD_SECURITY_CREDENTIAL_RESPONSE = """
            {
              "Code" : "Success",
              "LastUpdated" : "2012-04-26T16:39:16Z",
              "Type" : "AWS-HMAC",
              "AccessKeyId" : "ASIAIOSFODNN7EXAMPLE",
              "SecretAccessKey" : "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
              "Token" : "token",
              "Expiration" : "2017-05-17T15:09:54Z"
            }""";
    private static WireMockServer metadataServer;
    private Ec2MetadataCredentialsProviderConfig config;

    @BeforeAll
    public static void initMockRegistry() {
        metadataServer = new WireMockServer(wireMockConfig().dynamicPort());
        metadataServer.start();
    }

    @AfterAll
    public static void shutdownMockRegistry() {
        metadataServer.shutdown();
    }

    @BeforeEach
    void setUp() {
        config = new Ec2MetadataCredentialsProviderConfig(IAM_ROLE, URI.create(metadataServer.baseUrl()), 0.20);

        metadataServer.stubFor(
                put(urlEqualTo(TOKEN_RETRIEVAL_ENDPOINT))
                        .willReturn(WireMock.aResponse()
                                .withBody(MY_TOKEN)));

    }

    @AfterEach
    void afterEach() {
        metadataServer.resetAll();
    }

    @Test
    void rejectsNullConfig() {
        assertThatThrownBy(() -> new Ec2MetadataCredentialsProvider(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void credentialFromKnownGood() {

        metadataServer.stubFor(
                get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                        .willReturn(WireMock.aResponse()
                                .withBody(KNOWN_GOOD_SECURITY_CREDENTIAL_RESPONSE)));

        try (var provider = new Ec2MetadataCredentialsProvider(config)) {
            var credentialsStage = provider.getCredentials();
            assertThat(credentialsStage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .returns("ASIAIOSFODNN7EXAMPLE", SecurityCredentials::accessKeyId)
                    .returns("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", SecurityCredentials::secretAccessKey)
                    .returns(Instant.parse("2017-05-17T15:09:54Z"), SecurityCredentials::expiration);
        }
    }

    @Test
    void subsequentCallReturnsCachedCredential() {
        var now = Instant.now();
        var fixedClock = Clock.fixed(now, ZoneId.systemDefault());

        var credentials = createTestCredential("Success", "accessKeyId", "secretAccessKey", "token", Instant.now().plusSeconds(1));
        metadataServer.stubFor(
                get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                        .willReturn(WireMock.aResponse()
                                .withBody(toJson(credentials))));

        try (var provider = new Ec2MetadataCredentialsProvider(config, fixedClock)) {
            var credentialStage = provider.getCredentials();
            assertThat(credentialStage)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .isNotNull();

            var credential = credentialStage.toCompletableFuture().join();

            var again = provider.getCredentials();
            assertThat(again)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .isEqualTo(credential);
        }
    }

    /**
     * This test ensures that the credentials get refreshed, preemptively, before its expiration time.
     */
    @Test
    void credentialGetsPreemptivelyRefreshed() {
        var now = Instant.now();
        var initial = createTestCredential("Success", "accessKeyId", "initialKey", "token", now.plusSeconds(10));

        metadataServer.stubFor(
                get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                        .willReturn(WireMock.aResponse()
                                .withBody(toJson(initial))));

        try (var provider = new Ec2MetadataCredentialsProvider(config, Clock.systemUTC())) {
            var credentialStage = provider.getCredentials();
            assertThat(credentialStage)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .returns("initialKey", SecurityCredentials::secretAccessKey)
                    .isNotNull();

            var refreshed = createTestCredential("Success", "accessKeyId", "refreshedKey", "token", now.plusSeconds(20));
            metadataServer.stubFor(
                    get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                            .willReturn(WireMock.aResponse()
                                    .withBody(toJson(refreshed))));

            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> {
                        var refreshedStage = provider.getCredentials();
                        assertThat(refreshedStage)
                                .succeedsWithin(Duration.ofSeconds(1))
                                .returns("refreshedKey", SecurityCredentials::secretAccessKey);
                    });
        }
    }

    /**
     * This test ensures if a credential somehow expires (because time is beyond its expiration)
     * that it get refreshed anyway.
     */
    @Test
    void expiredCredentialRefreshed() {
        var factorSoLargePreemptiveRefreshBeAfterExpiry = 2.0;
        var cfg = new Ec2MetadataCredentialsProviderConfig(IAM_ROLE, config.metadataEndpoint(), factorSoLargePreemptiveRefreshBeAfterExpiry);
        var clock = mock(Clock.class);

        var now = Instant.now();
        var initial = createTestCredential("Success", "accessKeyId", "initialKey", "token", now.plusSeconds(10));

        metadataServer.stubFor(
                get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                        .willReturn(WireMock.aResponse()
                                .withBody(toJson(initial))));

        try (var provider = new Ec2MetadataCredentialsProvider(cfg, clock)) {
            var credentialStage = provider.getCredentials();
            assertThat(credentialStage)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .returns("initialKey", SecurityCredentials::secretAccessKey)
                    .isNotNull();

            var refreshed = createTestCredential("Success", "accessKeyId", "refreshedKey", "token", now.plusSeconds(20));
            metadataServer.stubFor(
                    get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                            .willReturn(WireMock.aResponse()
                                    .withBody(toJson(refreshed))));

            // advance time so that the initial credential has past its expiration.
            var timeBeyondInitialExpiry = initial.expiration().plusSeconds(1);
            when(clock.instant()).thenReturn(timeBeyondInitialExpiry);

            credentialStage = provider.getCredentials();
            assertThat(credentialStage)
                    .succeedsWithin(Duration.ofSeconds(1))
                    .returns("refreshedKey", SecurityCredentials::secretAccessKey);
        }
    }

    @Test
    void tokenRequestFails() {
        metadataServer.stubFor(
                put(urlEqualTo(TOKEN_RETRIEVAL_ENDPOINT))
                        .willReturn(WireMock.aResponse()
                                .withStatus(500)));

        try (var provider = new Ec2MetadataCredentialsProvider(config)) {
            var result = provider.getCredentials();
            assertThat(result)
                    .failsWithin(Duration.ofSeconds(1))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("HTTP status code 500");
        }
    }

    @Test
    void tokenRequest() {

        var credential = createTestCredential("Fail", "accessKeyId", "secretAccessKey", "token", Clock.systemUTC().instant().plusSeconds(30));
        metadataServer.stubFor(
                get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                        .willReturn(WireMock.aResponse()
                                .withBody(toJson(credential))));

        try (var provider = new Ec2MetadataCredentialsProvider(config)) {
            var result = provider.getCredentials();
            assertThat(result)
                    .failsWithin(Duration.ofSeconds(1))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("Unexpected code value in SecurityCredentials object returned from AWS");
        }
    }

    @Test
    void securityCredentialRequestFails() {
        metadataServer.stubFor(
                get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                        .willReturn(WireMock.aResponse()
                                .withStatus(500)));

        try (var provider = new Ec2MetadataCredentialsProvider(config)) {
            var result = provider.getCredentials();
            assertThat(result)
                    .failsWithin(Duration.ofSeconds(1))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("HTTP status code 500");
        }
    }

    @Test
    void securityCredentialRetrievedAfterRequestFails() {
        metadataServer.stubFor(
                get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                        .willReturn(WireMock.aResponse()
                                .withStatus(500)));

        try (var provider = new Ec2MetadataCredentialsProvider(config)) {
            var result = provider.getCredentials();
            assertThat(result)
                    .failsWithin(Duration.ofSeconds(1))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("HTTP status code 500");

            var credential = createTestCredential("Success", "accessKeyId", "secretAccessKey", "token", Clock.systemUTC().instant().plusSeconds(30));
            metadataServer.stubFor(
                    get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                            .willReturn(WireMock.aResponse()
                                    .withBody(toJson(credential))));

            result = provider.getCredentials();
            assertThat(result)
                    .succeedsWithin(Duration.ofSeconds(2))
                    .returns("secretAccessKey", SecurityCredentials::secretAccessKey);
        }
    }

    @NonNull
    private SecurityCredentials createTestCredential(String code, String accessKey, String secretKey, String token, Instant expiration) {
        return new SecurityCredentials(code, accessKey, secretKey, token, expiration);
    }

    private byte[] toJson(SecurityCredentials credentials) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(credentials);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
