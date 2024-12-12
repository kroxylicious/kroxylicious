/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.kroxylicious.kms.provider.fortanix.dsm.config.ApiKeySessionProviderConfig;
import io.kroxylicious.kms.provider.fortanix.dsm.config.Config;
import io.kroxylicious.kms.provider.fortanix.dsm.session.Session;
import io.kroxylicious.kms.provider.fortanix.dsm.session.SessionProvider;
import io.kroxylicious.kms.provider.fortanix.dsm.session.SessionProviderFactory;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.kms.service.SecretKeyUtils;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import edu.umd.cs.findbugs.annotations.NonNull;

import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link FortanixDsmKms}.  See also io.kroxylicious.kms.service.KmsIT.
 */
class FortanixDsmKmsTest {

    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
    private static final String KEYS_ENDPOINT = "/crypto/v1/keys";
    private static final String KEYS_INFO_ENDPOINT = "/crypto/v1/keys/info";
    private static final String KEYS_EXPORT_ENDPOINT = "/crypto/v1/keys/export";
    private static final String KEYS_BATCH_ENCRYPT_ENDPOINT = "crypto/v1/keys/batch/encrypt";
    private static WireMockServer server;
    private Config config;
    private FortanixDsmKmsService kmsService;
    private FortanixDsmKms kms;

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
        config = new Config(URI.create(server.baseUrl()), new ApiKeySessionProviderConfig(new InlinePassword("apiKey"), 0.80), null);

        kmsService = new FortanixDsmKmsService(new SessionProviderFactory() {
            @NonNull
            @Override
            public SessionProvider createSessionProvider(@NonNull Config config) {
                var session = new Session() {

                    @NonNull
                    @Override
                    public String authorizationHeader() {
                        return "Bearer auth";
                    }

                    @NonNull
                    @Override
                    public Instant expiration() {
                        return Instant.MAX;
                    }
                };
                var sessionStage = CompletableFuture.<Session> completedStage(session);
                return new SessionProvider() {
                    @NonNull
                    @Override
                    public CompletionStage<Session> getSession() {
                        return sessionStage;
                    }
                };
            }
        });
        kmsService.initialize(config);
        kms = kmsService.buildKms();

        // server.stubFor(
        // post(urlEqualTo(SESSION_AUTH_ENDPOINT))
        // .willReturn(WireMock.aResponse()
        // .withBody(SESSION_AUTH_RESPONSE)));

    }

    @AfterEach
    void afterEach() {
        Optional.ofNullable(kmsService).ifPresent(KmsService::close);
        server.resetAll();
    }

    @Test
    void resolveAlias() {
        var response = """
                {
                    "kid": "1234abcd-12ab-34cd-56ef-1234567890ab"
                }
                """;

        server.stubFor(post(urlEqualTo(KEYS_INFO_ENDPOINT)).willReturn(
                WireMock.aResponse().withBody(response)));

        var expectedKeyId = "1234abcd-12ab-34cd-56ef-1234567890ab";

        var aliasStage = kms.resolveAlias("alias");
        assertThat(aliasStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(expectedKeyId);
    }

    @Test
    void resolveAliasNotFound() {

        server.stubFor(post(urlEqualTo(KEYS_INFO_ENDPOINT))
                .willReturn(WireMock.aResponse().withStatus(404)));

        var aliasStage = kms.resolveAlias("alias");
        assertThat(aliasStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownAliasException.class)
                .withMessageContaining("alias");

    }

    @Test
    void resolveAliasInternalServerError() {
        server.stubFor(post(urlEqualTo(KEYS_INFO_ENDPOINT))
                .willReturn(WireMock.aResponse().withStatus(500)));

        var aliasStage = kms.resolveAlias("alias");
        assertThat(aliasStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(KmsException.class)
                .withMessageContaining("crypto/v1/keys/info, HTTP status code 500");

    }

    @Test
    @Disabled
    void generateDekPair() {

        var infoResponse = """
                {
                    "kid": "1234abcd-12ab-34cd-56ef-1234567890ab"
                }
                """;

        server.stubFor(post(urlEqualTo(KEYS_INFO_ENDPOINT)).willReturn(
                WireMock.aResponse().withBody(infoResponse)));

        var createTmpKeyResponse = """
                {
                    "transient_key":"SlmFdhHoCP8G4U81WQuZAfVaDumf3xBAzhEDHQc7WjSBpQBAAQICWMwwMN49eIlFggkLLqdgGJtecC3LWFMr1jEq/GiJNo39xOGL6AaN8fIz82fH2JUJIvMxtDRzfBWulitHjC5g9DMRBxWIR0vMW32yIBWuKPAgbiSKmDK9j6L7XGy6oKVBJgMLdQRgOuczriQiKjRuVaQAHvRzx9Fnj6jQPy6jOCkw3hmRs3aBCSmr5WsICgLeBbqHqO7mi/E/JJux6y/I+AGw1RBqAbHGeeIV7zLV+4Krbi46ITDVSptqSzvgn8k7X6BwpnKGPO3rUifP2TEDUB+YpqOct/LGOO0MEBICgZsEUKh19IKwTF05A3D8a6lfe/c="
                }
                """;
        server.stubFor(post(urlEqualTo(KEYS_ENDPOINT)).willReturn(
                WireMock.aResponse().withBody(createTmpKeyResponse)));

        var exportResponse = """
                {
                        "transient_key":"SlmFdhHoCP8G4U81WQuZAfVaDumf3xBAzhEDHQc7WjSBpQBAAQICWMwwMN49eIlFggkLLqdgGJtecC3LWFMr1jEq/GiJNo39xOGL6AaN8fIz82fH2JUJIvMxtDRzfBWulitHjC5g9DMRBxWIR0vMW32yIBWuKPAgbiSKmDK9j6L7XGy6oKVBJgMLdQRgOuczriQiKjRuVaQAHvRzx9Fnj6jQPy6jOCkw3hmRs3aBCSmr5WsICgLeBbqHqO7mi/E/JJux6y/I+AGw1RBqAbHGeeIV7zLV+4Krbi46ITDVSptqSzvgn8k7X6BwpnKGPO3rUifP2TEDUB+YpqOct/LGOO0MEBICgZsEUKh19IKwTF05A3D8a6lfe/c=",
                        "value":"HfYxDpn0FFajKx6TGYw+9P1n0rRbKwbtao7sdr8eXi4="}
                """;
        server.stubFor(post(urlEqualTo(KEYS_EXPORT_ENDPOINT)).willReturn(
                WireMock.aResponse().withBody(exportResponse)));

        var batchEncryptResponse = """
                {
                        "transient_key":"SlmFdhHoCP8G4U81WQuZAfVaDumf3xBAzhEDHQc7WjSBpQBAAQICWMwwMN49eIlFggkLLqdgGJtecC3LWFMr1jEq/GiJNo39xOGL6AaN8fIz82fH2JUJIvMxtDRzfBWulitHjC5g9DMRBxWIR0vMW32yIBWuKPAgbiSKmDK9j6L7XGy6oKVBJgMLdQRgOuczriQiKjRuVaQAHvRzx9Fnj6jQPy6jOCkw3hmRs3aBCSmr5WsICgLeBbqHqO7mi/E/JJux6y/I+AGw1RBqAbHGeeIV7zLV+4Krbi46ITDVSptqSzvgn8k7X6BwpnKGPO3rUifP2TEDUB+YpqOct/LGOO0MEBICgZsEUKh19IKwTF05A3D8a6lfe/c=",
                        "value":"HfYxDpn0FFajKx6TGYw+9P1n0rRbKwbtao7sdr8eXi4="}
                """;
        server.stubFor(post(urlEqualTo(KEYS_BATCH_ENCRYPT_ENDPOINT)).willReturn(
                WireMock.aResponse().withBody(exportResponse)));

        var ciphertextBlobBytes = BASE64_DECODER.decode(
                "AQEDAHjRYf5WytIc0C857tFSnBaPn2F8DgfmThbJlGfR8P3WlwAAAH4wfAYJKoZIhvcNAQcGoG8wbQIBADBoBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDEFogLqPWZconQhwHAIBEIA7d9AC7GeJJM34njQvg4Wf1d5sw0NIo1MrBqZa+YdhV8MrkBQPeac0ReRVNDt9qleAt+SHgIRF8P0H+7U=");
        var plainTextBytes = BASE64_DECODER.decode("VdzKNHGzUAzJeRBVY+uUmofUGGiDzyB3+i9fVkh3piw=");
        var expectedKey = DestroyableRawSecretKey.takeCopyOf(plainTextBytes, "AES");

        var dekPairStage = kms.generateDekPair("kekRef");
        assertThat(dekPairStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .extracting(DekPair::edek)
                .isEqualTo(new FortanixDsmKmsEdek("alias", ciphertextBlobBytes, null));

        assertThat(dekPairStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .extracting(DekPair::dek)
                .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                .matches(key -> SecretKeyUtils.same(key, expectedKey));

        //
        // // response from https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.html
        // var response = """
        // {
        // "CiphertextBlob":
        // "AQEDAHjRYf5WytIc0C857tFSnBaPn2F8DgfmThbJlGfR8P3WlwAAAH4wfAYJKoZIhvcNAQcGoG8wbQIBADBoBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDEFogLqPWZconQhwHAIBEIA7d9AC7GeJJM34njQvg4Wf1d5sw0NIo1MrBqZa+YdhV8MrkBQPeac0ReRVNDt9qleAt+SHgIRF8P0H+7U=",
        // "KeyId": "arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
        // "Plaintext": "VdzKNHGzUAzJeRBVY+uUmofUGGiDzyB3+i9fVkh3piw="
        // }
        // """;
        // var ciphertextBlobBytes = BASE64_DECODER.decode(
        // "AQEDAHjRYf5WytIc0C857tFSnBaPn2F8DgfmThbJlGfR8P3WlwAAAH4wfAYJKoZIhvcNAQcGoG8wbQIBADBoBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDEFogLqPWZconQhwHAIBEIA7d9AC7GeJJM34njQvg4Wf1d5sw0NIo1MrBqZa+YdhV8MrkBQPeac0ReRVNDt9qleAt+SHgIRF8P0H+7U=");
        // var plainTextBytes = BASE64_DECODER.decode("VdzKNHGzUAzJeRBVY+uUmofUGGiDzyB3+i9fVkh3piw=");
        // var expectedKey = DestroyableRawSecretKey.takeCopyOf(plainTextBytes, "AES");

        withMockAwsWithSingleResponse(exportResponse, vaultKms -> {
            var aliasStage = vaultKms.generateDekPair("alias");
            assertThat(aliasStage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .extracting(DekPair::edek)
                    .isEqualTo(new FortanixDsmKmsEdek("alias", ciphertextBlobBytes, null));

            assertThat(aliasStage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .extracting(DekPair::dek)
                    .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                    .matches(key -> SecretKeyUtils.same(key, expectedKey));
        });
    }

    @Test
    @Disabled
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
            Assertions.assertThat(kms.decryptEdek(new FortanixDsmKmsEdek("kek", "unused".getBytes(StandardCharsets.UTF_8), null)))
                    .succeedsWithin(Duration.ofSeconds(5))
                    .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                    .matches(key -> SecretKeyUtils.same(key, expectedKey));
        });
    }

    void withMockAwsWithSingleResponse(String response, Consumer<FortanixDsmKms> consumer) {
        withMockAwsWithSingleResponse(response, 200, consumer);
    }

    void withMockAwsWithSingleResponse(String response, int statusCode, Consumer<FortanixDsmKms> consumer) {
        HttpHandler handler = statusCode >= 500 ? new ErrorResponse(statusCode) : new StaticResponse(statusCode, response);
        HttpServer httpServer = httpServer(handler);
        try {
            var address = httpServer.getAddress();
            var awsAddress = "http://127.0.0.1:" + address.getPort();
            var config = new Config(URI.create(awsAddress), new ApiKeySessionProviderConfig(new InlinePassword("access"), 0.8), null);
            @SuppressWarnings("resource")
            var awsKmsService = new FortanixDsmKmsService();
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
