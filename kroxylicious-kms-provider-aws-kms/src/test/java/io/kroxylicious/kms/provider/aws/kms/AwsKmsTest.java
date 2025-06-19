/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.provider.aws.kms.config.LongTermCredentialsProviderConfig;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.SecretKeyUtils;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link AwsKms}.  See also io.kroxylicious.kms.service.KmsIT.
 */
class AwsKmsTest {

    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

    private static WireMockServer server;
    private AwsKmsService awsKmsService;
    private AwsKms kms;

    @BeforeAll
    public static void initMockServer() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
    }

    @AfterAll
    public static void shutdownMockServer() {
        server.shutdown();
    }

    @BeforeEach
    public void beforeEach() {
        var longTermCredentialsProviderConfig = new LongTermCredentialsProviderConfig(new InlinePassword("access"), new InlinePassword("secret"));
        var config = new Config(URI.create(server.baseUrl()), longTermCredentialsProviderConfig, null, "us-west-2", null);
        awsKmsService = new AwsKmsService();
        awsKmsService.initialize(config);
        kms = awsKmsService.buildKms();
    }

    @AfterEach
    void afterEach() {
        Optional.ofNullable(awsKmsService).ifPresent(AwsKmsService::close);
        server.resetAll();
    }

    @Test
    void resolveAlias() {
        // example response from https://docs.aws.amazon.com/kms/latest/APIReference/API_DescribeKey.html
        var response = """
                {
                    "KeyMetadata": {
                        "KeyId": "1234abcd-12ab-34cd-56ef-1234567890ab",
                        "Arn": "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab"
                    }
                }
                """;
        var expectedKeyId = "1234abcd-12ab-34cd-56ef-1234567890ab";

        server.stubFor(
                post(urlEqualTo("/"))
                        .withHeader("X-Amz-Target", equalTo("TrentService.DescribeKey"))
                        .withRequestBody(matchingJsonPath("$.KeyId", equalTo("alias/alias")))
                        .willReturn(aResponse().withBody(response)));

        var aliasStage = kms.resolveAlias("alias");
        assertThat(aliasStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(expectedKeyId);
    }

    @Test
    void resolveAliasNotFound() {
        var response = """
                {
                    "__type": "NotFoundException",
                    "message": "Invalid keyId 'foo'"}
                }
                """;

        server.stubFor(
                post(urlEqualTo("/"))
                        .withHeader("X-Amz-Target", equalTo("TrentService.DescribeKey"))
                        .willReturn(aResponse().withBody(response).withStatus(400)));

        var aliasStage = kms.resolveAlias("alias");
        assertThat(aliasStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownAliasException.class)
                .withMessageContaining("key 'alias' is not found");
    }

    @Test
    void resolveAliasInternalServerError() {
        server.stubFor(
                post(urlEqualTo("/"))
                        .withHeader("X-Amz-Target", equalTo("TrentService.DescribeKey"))
                        .willReturn(aResponse().withStatus(500)));

        var aliasStage = kms.resolveAlias("alias");
        assertThat(aliasStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(KmsException.class)
                .withMessageContaining("Operation failed");
    }

    @Test
    void generateDekPair() {
        // response from https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.html
        var response = """
                {
                  "CiphertextBlob": "AQEDAHjRYf5WytIc0C857tFSnBaPn2F8DgfmThbJlGfR8P3WlwAAAH4wfAYJKoZIhvcNAQcGoG8wbQIBADBoBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDEFogLqPWZconQhwHAIBEIA7d9AC7GeJJM34njQvg4Wf1d5sw0NIo1MrBqZa+YdhV8MrkBQPeac0ReRVNDt9qleAt+SHgIRF8P0H+7U=",
                  "KeyId": "arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
                  "Plaintext": "VdzKNHGzUAzJeRBVY+uUmofUGGiDzyB3+i9fVkh3piw="
                }
                """;

        server.stubFor(
                post(urlEqualTo("/"))
                        .withHeader("X-Amz-Target", equalTo("TrentService.GenerateDataKey"))
                        .withRequestBody(matchingJsonPath("$.KeyId", equalTo("kek")))
                        .willReturn(aResponse().withBody(response)));

        var ciphertextBlobBytes = BASE64_DECODER.decode(
                "AQEDAHjRYf5WytIc0C857tFSnBaPn2F8DgfmThbJlGfR8P3WlwAAAH4wfAYJKoZIhvcNAQcGoG8wbQIBADBoBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDEFogLqPWZconQhwHAIBEIA7d9AC7GeJJM34njQvg4Wf1d5sw0NIo1MrBqZa+YdhV8MrkBQPeac0ReRVNDt9qleAt+SHgIRF8P0H+7U=");
        var plainTextBytes = BASE64_DECODER.decode("VdzKNHGzUAzJeRBVY+uUmofUGGiDzyB3+i9fVkh3piw=");
        var expectedKey = DestroyableRawSecretKey.takeCopyOf(plainTextBytes, "AES");

        var dekStage = kms.generateDekPair("kek");
        assertThat(dekStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .satisfies(dekPair -> {
                    assertThat(dekPair)
                            .extracting(DekPair::edek)
                            .isEqualTo(new AwsKmsEdek("kek", ciphertextBlobBytes));

                    assertThat(dekPair)
                            .extracting(DekPair::dek)
                            .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                            .matches(key -> SecretKeyUtils.same(key, expectedKey));
                });
    }

    @Test
    void decryptEdek() {
        var plainTextBytes = BASE64_DECODER.decode("VGhpcyBpcyBEYXkgMSBmb3IgdGhlIEludGVybmV0Cg==");
        // response from https://docs.aws.amazon.com/kms/latest/APIReference/API_Decrypt.html
        var response = """
                {
                  "KeyId": "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
                  "Plaintext": "VGhpcyBpcyBEYXkgMSBmb3IgdGhlIEludGVybmV0Cg==",
                  "EncryptionAlgorithm": "SYMMETRIC_DEFAULT"
                }""";

        server.stubFor(
                post(urlEqualTo("/"))
                        .withHeader("X-Amz-Target", equalTo("TrentService.Decrypt"))
                        .withRequestBody(matchingJsonPath("$.KeyId", equalTo("kek")))
                        .willReturn(aResponse().withBody(response)));

        var expectedKey = DestroyableRawSecretKey.takeCopyOf(plainTextBytes, "AES");

        var keyStage = kms.decryptEdek(new AwsKmsEdek("kek", "unused".getBytes(StandardCharsets.UTF_8)));
        assertThat(keyStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                .matches(key -> SecretKeyUtils.same(key, expectedKey));
    }
}
