/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.net.URI;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.service.TestKekManager;

import edu.umd.cs.findbugs.annotations.NonNull;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

class AwsKmsTestKmsFacadeTest {
    private static WireMockServer server;
    private AwsKmsTestKmsFacadeWireMock facade;
    private TestKekManager manager;

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
        var keyId = "1234abcd-12ab-34cd-56ef-1234567890ab";

        var response = """
                {
                    "KeyMetadata": {
                        "KeyId": "1234abcd-12ab-34cd-56ef-1234567890ab",
                        "Arn": "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab"
                    }
                }
                """;

        server.stubFor(
                post(urlEqualTo("/"))
                        .withHeader("X-Amz-Target", equalTo("TrentService.DescribeKey"))
                        .withRequestBody(matchingJsonPath("$.KeyId", equalTo("alias/alias")))
                        .willReturn(aResponse().withBody(response)));

        var rotateResponse = """
                {"KeyId": "1234abcd-12ab-34cd-56ef-1234567890ab"}
                """;

        server.stubFor(
                post(urlEqualTo("/"))
                        .withHeader("X-Amz-Target", equalTo("TrentService.RotateKeyOnDemand"))
                        .withRequestBody(matchingJsonPath("$.KeyId", equalTo(keyId)))
                        .willReturn(aResponse().withBody(rotateResponse)));

        facade = new AwsKmsTestKmsFacadeWireMock(URI.create(server.baseUrl()), Optional.of("us-west-2"), Optional.of("test-access-key"), Optional.of("test-secret-key"));
        facade.start();
        manager = facade.getTestKekManager();
    }

    @AfterEach
    void afterEach() {
        try {
            facade.stop();
            server.resetAll();
        }
        finally {
            Optional.ofNullable(facade).ifPresent(AwsKmsTestKmsFacadeWireMock::close);
        }
    }

    @Test
    void rotateKekAWS() {
        var keyId = "1234abcd-12ab-34cd-56ef-1234567890ab";
        var alias = "alias";

        var response = """
                {
                    "Rotations": [
                        {
                          "KeyId": "1234abcd-12ab-34cd-56ef-1234567890ab",
                          "RotationDate": "2024-03-02T10:11:36.564000+00:00",
                          "RotationType": "AUTOMATIC"
                        },
                        {
                          "KeyId": "1234abcd-12ab-34cd-56ef-1234567890ab",
                          "RotationDate":  "2024-04-05T15:14:47.757000+00:00",
                          "RotationType": "ON_DEMAND"
                        }
                    ],
                  "Truncated": false
                }
                """;

        server.stubFor(
                post(urlEqualTo("/"))
                        .withHeader("X-Amz-Target", equalTo("TrentService.ListKeyRotations"))
                        .withRequestBody(matchingJsonPath("$.KeyId", equalTo(keyId)))
                        .willReturn(aResponse().withBody(response)));

        manager.rotateKek(alias);

        server.verify(3, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void rotateKekLocalStack() {
        var alias = "alias";
        var description = "[rotated] key for alias: " + alias;
        var keyId = "1234abcd-12ab-34cd-56ef-1234567890ab";

        server.stubFor(
                post(urlEqualTo("/"))
                        .withHeader("X-Amz-Target", equalTo("TrentService.ListKeyRotations"))
                        .willReturn(aResponse().withStatus(501)));

        var response = """
                {
                  "KeyMetadata": {
                    "AWSAccountId": "111122223333",
                    "Arn": "arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
                    "CreationDate": 1.499288695918E9,
                    "CustomerMasterKeySpec": "SYMMETRIC_DEFAULT",
                    "Description": "",
                    "Enabled": true,
                    "EncryptionAlgorithms": [
                        "SYMMETRIC_DEFAULT"
                    ],
                    "KeyId": "1234abcd-12ab-34cd-56ef-1234567890ab",
                    "KeyManager": "CUSTOMER",
                    "KeySpec": "SYMMETRIC_DEFAULT",
                    "KeyState": "Enabled",
                    "KeyUsage": "ENCRYPT_DECRYPT",
                    "MultiRegion": false,
                    "Origin": "AWS_KMS"
                  }
                }
                """;

        server.stubFor(
                post(urlEqualTo("/"))
                        .withHeader("X-Amz-Target", equalTo("TrentService.CreateKey"))
                        .withRequestBody(matchingJsonPath("$.description", equalTo(description)))
                        .willReturn(aResponse().withBody(response)));

        server.stubFor(
                post(urlEqualTo("/"))
                        .withHeader("X-Amz-Target", equalTo("TrentService.UpdateAlias"))
                        .withRequestBody(matchingJsonPath("$.TargetKeyId", equalTo(keyId)))
                        .willReturn(aResponse().withStatus(200)));

        manager.rotateKek(alias);

        server.verify(4, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void classAndConfig() {
        assertThat(facade.getKmsServiceClass()).isEqualTo(AwsKmsService.class);
        assertThat(facade.getKmsServiceConfig()).isInstanceOf(Config.class);
    }

    /**
     * AwsKmsTestKmsFacade class for WireMock
     */
    static class AwsKmsTestKmsFacadeWireMock extends AbstractAwsKmsTestKmsFacade {

        private final URI uri;
        private final Optional<String> region;
        private final Optional<String> accessKey;
        private final Optional<String> secretKey;

        AwsKmsTestKmsFacadeWireMock(URI uri, Optional<String> region, Optional<String> accessKey, Optional<String> secretKey) {
            this.uri = uri;
            this.region = region;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
        }

        @Override
        protected void startKms() {
            // We don't require the implementation for this method since we are using WireMock
        }

        @Override
        protected void stopKms() {
            // We don't require the implementation for this method since we are using WireMock
        }

        @Override
        @NonNull
        protected URI getAwsUrl() {
            return uri;
        }

        @Override
        protected String getRegion() {
            return region.orElseThrow();
        }

        @Override
        protected String getSecretKey() {
            return accessKey.orElseThrow();
        }

        @Override
        protected String getAccessKey() {
            return secretKey.orElseThrow();
        }
    }
}
