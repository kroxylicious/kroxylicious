/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;

import io.kroxylicious.kms.provider.aws.kms.config.Ec2CredentialsProviderConfig;
import io.kroxylicious.kms.provider.aws.kms.credentials.Ec2CredentialsProvider.SecurityCredentials;
import io.kroxylicious.kms.service.KmsException;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.kroxylicious.kms.provider.aws.kms.credentials.Ec2CredentialsProvider.META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT;
import static io.kroxylicious.kms.provider.aws.kms.credentials.Ec2CredentialsProvider.TOKEN_RETRIEVAL_ENDPOINT;
import static org.assertj.core.api.Assertions.assertThat;

class Ec2CredentialsProviderTest {

    private static final String IAM_ROLE = "myrole";

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

    @BeforeAll
    public static void initMockRegistry() {
        metadataServer = new WireMockServer(wireMockConfig().dynamicPort());

        metadataServer.start();

    }

    @AfterAll
    public static void shutdownMockRegistry() {
        metadataServer.shutdown();
    }

    @Test
    void credentialFromKnownGood() {
        metadataServer.stubFor(
                put(urlEqualTo(TOKEN_RETRIEVAL_ENDPOINT))
                        .willReturn(WireMock.aResponse()
                                .withBody("mytoken")));

        metadataServer.stubFor(
                get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                        .willReturn(WireMock.aResponse()
                                .withBody(KNOWN_GOOD_SECURITY_CREDENTIAL_RESPONSE)));

        try (var provider = new Ec2CredentialsProvider(new Ec2CredentialsProviderConfig(IAM_ROLE, URI.create(metadataServer.baseUrl())))) {
            var credentialsStage = provider.getCredentials();
            assertThat(credentialsStage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .returns("ASIAIOSFODNN7EXAMPLE", SecurityCredentials::accessKey)
                    .returns("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", SecurityCredentials::secretKey)
                    .returns(Instant.parse("2017-05-17T15:09:54Z"), SecurityCredentials::expiration);
        }
    }

    @Test
    void returnsCachedCredential() {
        metadataServer.stubFor(
                put(urlEqualTo(TOKEN_RETRIEVAL_ENDPOINT))
                        .willReturn(WireMock.aResponse()
                                .withBody("mytoken")));

        metadataServer.stubFor(
                get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                        .willReturn(WireMock.aResponse()
                                .withBody(KNOWN_GOOD_SECURITY_CREDENTIAL_RESPONSE)));

        try (var provider = new Ec2CredentialsProvider(new Ec2CredentialsProviderConfig(IAM_ROLE, URI.create(metadataServer.baseUrl())))) {
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

    @Test
    void expiredCredentialRefreshed() {
        metadataServer.stubFor(
                put(urlEqualTo(TOKEN_RETRIEVAL_ENDPOINT))
                        .willReturn(WireMock.aResponse()
                                .withBody("mytoken")));

        metadataServer.stubFor(
                get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                        .willReturn(WireMock.aResponse()
                                .withBody("""
                                        {
                                          "Code" : "Success",
                                          "LastUpdated" : "2012-04-26T16:39:16Z",
                                          "Type" : "AWS-HMAC",
                                          "AccessKeyId" : "ASIAIOSFODNN7EXAMPLE",
                                          "SecretAccessKey" : "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                                          "Token" : "token",
                                          "Expiration" : "2027-05-17T15:09:54Z"
                                        }""")));

        try (var provider = new Ec2CredentialsProvider(new Ec2CredentialsProviderConfig(IAM_ROLE, URI.create(metadataServer.baseUrl())))) {
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

    @Test
    void tokenRequestFails() {
        metadataServer.stubFor(
                put(urlEqualTo(TOKEN_RETRIEVAL_ENDPOINT))
                        .willReturn(WireMock.aResponse()
                                .withStatus(500)));

        try (var provider = new Ec2CredentialsProvider(new Ec2CredentialsProviderConfig(IAM_ROLE, URI.create(metadataServer.baseUrl())))) {
            var result = provider.getCredentials();
            assertThat(result)
                    .failsWithin(Duration.ofSeconds(1))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("HTTP status code 500");
        }
    }

    @Test
    void securityCredentialRequestFails() {
        metadataServer.stubFor(
                put(urlEqualTo(TOKEN_RETRIEVAL_ENDPOINT))
                        .willReturn(WireMock.aResponse()
                                .withBody("mytoken")));

        metadataServer.stubFor(
                get(urlEqualTo(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + IAM_ROLE))
                        .willReturn(WireMock.aResponse()
                                .withStatus(500)));

        try (var provider = new Ec2CredentialsProvider(new Ec2CredentialsProviderConfig(IAM_ROLE, URI.create(metadataServer.baseUrl())))) {
            var result = provider.getCredentials();
            assertThat(result)
                    .failsWithin(Duration.ofSeconds(1))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("HTTP status code 500");
        }
    }

}
