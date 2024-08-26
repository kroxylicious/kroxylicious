/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.provider.aws.kms.config.FixedCredentialsProviderConfig;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.SecretKeyUtils;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link AwsKms}.  See also io.kroxylicious.kms.service.KmsIT.
 */
class AwsKmsTest {

    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

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
        withMockAwsWithSingleResponse(response, kms -> {
            var aliasStage = kms.resolveAlias("alias");
            assertThat(aliasStage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .isEqualTo(expectedKeyId);
        });
    }

    @Test
    void resolveAliasNotFound() {
        var response = """
                {
                    "__type": "NotFoundException",
                    "message": "Invalid keyId 'foo'"}
                }
                """;
        withMockAwsWithSingleResponse(response, 400, kms -> {
            var aliasStage = kms.resolveAlias("alias");
            assertThat(aliasStage)
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .withCauseInstanceOf(UnknownAliasException.class)
                    .withMessageContaining("key 'alias' is not found");
        });
    }

    @Test
    void resolveAliasInternalServerError() {
        withMockAwsWithSingleResponse(null, 500, kms -> {
            var aliasStage = kms.resolveAlias("alias");
            assertThat(aliasStage)
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("Operation failed");
        });
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
        var ciphertextBlobBytes = BASE64_DECODER.decode(
                "AQEDAHjRYf5WytIc0C857tFSnBaPn2F8DgfmThbJlGfR8P3WlwAAAH4wfAYJKoZIhvcNAQcGoG8wbQIBADBoBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDEFogLqPWZconQhwHAIBEIA7d9AC7GeJJM34njQvg4Wf1d5sw0NIo1MrBqZa+YdhV8MrkBQPeac0ReRVNDt9qleAt+SHgIRF8P0H+7U=");
        var plainTextBytes = BASE64_DECODER.decode("VdzKNHGzUAzJeRBVY+uUmofUGGiDzyB3+i9fVkh3piw=");
        var expectedKey = DestroyableRawSecretKey.takeCopyOf(plainTextBytes, "AES");

        withMockAwsWithSingleResponse(response, awsKms -> {
            var aliasStage = awsKms.generateDekPair("alias");
            assertThat(aliasStage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .extracting(DekPair::edek)
                    .isEqualTo(new AwsKmsEdek("alias", ciphertextBlobBytes));

            assertThat(aliasStage)
                    .succeedsWithin(Duration.ofSeconds(5))
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
        var expectedKey = DestroyableRawSecretKey.takeCopyOf(plainTextBytes, "AES");

        withMockAwsWithSingleResponse(response, kms -> {
            Assertions.assertThat(kms.decryptEdek(new AwsKmsEdek("kek", "unused".getBytes(StandardCharsets.UTF_8))))
                    .succeedsWithin(Duration.ofSeconds(5))
                    .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                    .matches(key -> SecretKeyUtils.same(key, expectedKey));
        });
    }

    void withMockAwsWithSingleResponse(String response, Consumer<AwsKms> consumer) {
        withMockAwsWithSingleResponse(response, 200, consumer);
    }

    void withMockAwsWithSingleResponse(String response, int statusCode, Consumer<AwsKms> consumer) {
        HttpHandler handler = statusCode >= 500 ? new ErrorResponse(statusCode) : new StaticResponse(statusCode, response);
        HttpServer httpServer = httpServer(handler);
        try {
            var address = httpServer.getAddress();
            var awsAddress = "http://127.0.0.1:" + address.getPort();
            var credentialsProvider = new FixedCredentialsProviderConfig(new InlinePassword("access"), new InlinePassword("secret"));
            var config = new Config(URI.create(awsAddress), credentialsProvider, "us-west-2", null);
            var awsKmsService = new AwsKmsService();
            awsKmsService.initialize(config);
            var service = awsKmsService.buildKms();
            consumer.accept(service);
        }
        finally {
            httpServer.stop(0);
        }
    }

    public static HttpServer httpServer(HttpHandler handler) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/", handler);
            server.start();
            return server;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private record StaticResponse(int statusCode, String response) implements HttpHandler {
        @Override
        public void handle(HttpExchange e) throws IOException {
            e.sendResponseHeaders(statusCode, response.length());
            try (var os = e.getResponseBody()) {
                os.write(response.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private record ErrorResponse(int statusCode) implements HttpHandler {
        @Override
        public void handle(HttpExchange e) throws IOException {
            e.sendResponseHeaders(500, -1);
            e.close();
        }
    }

}
