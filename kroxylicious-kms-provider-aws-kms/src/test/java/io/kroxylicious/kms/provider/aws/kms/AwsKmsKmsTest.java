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
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.SecretKeyUtils;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link AwsKmsKms}.  See also io.kroxylicious.kms.service.KmsIT.
 */
class AwsKmsKmsTest {

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
        withMockAwsWithSingleResponse(response, kms -> {
            var aliasStage = kms.resolveAlias("alias");
            assertThat(aliasStage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .isEqualTo("1234abcd-12ab-34cd-56ef-1234567890ab");
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

        withMockAwsWithSingleResponse(response, vaultKms -> {
            var aliasStage = vaultKms.generateDekPair("alias");
            assertThat(aliasStage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .extracting(DekPair::edek)
                    .isEqualTo(new AwsKmsEdek("alias", ciphertextBlobBytes));

            assertThat(aliasStage)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .extracting(DekPair::dek)
                    .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                    .matches(key -> SecretKeyUtils.same(key, DestroyableRawSecretKey.takeCopyOf(plainTextBytes, "AES")));
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
        withMockAwsWithSingleResponse(response, kms -> {
            Assertions.assertThat(kms.decryptEdek(new AwsKmsEdek("kek", "unused".getBytes(StandardCharsets.UTF_8))))
                    .succeedsWithin(Duration.ofSeconds(5))
                    .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                    .matches(key -> SecretKeyUtils.same(key, DestroyableRawSecretKey.takeCopyOf(plainTextBytes, "AES")));
        });
    }

    //
    // @Test
    // void resolveWithUnknownKeyReusesConnection() {
    // assertReusesConnectionsOn404(vaultKms -> {
    // assertThat(vaultKms.resolveAlias("alias")).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
    // .withCauseInstanceOf(UnknownAliasException.class);
    // });
    // }
    //
    // @Test
    // void generatedDekPairWithUnknownKeyReusesConnection() {
    // assertReusesConnectionsOn404(vaultKms -> {
    // assertThat(vaultKms.generateDekPair("alias")).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
    // .withCauseInstanceOf(UnknownKeyException.class);
    // });
    // }
    //
    //
    // @Test
    // void decryptEdekWithUnknownKeyReusesConnection() {
    // assertReusesConnectionsOn404(vaultKms -> {
    // assertThat(vaultKms.decryptEdek(new VaultEdek("unknown", new byte[]{ 1 }))).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
    // .withCauseInstanceOf(UnknownKeyException.class);
    // });
    // }
    //
    // @Test
    // void decryptEdek() {
    // String edek = "dGhlIHF1aWNrIGJyb3duIGZveAo=";
    // byte[] edekBytes = Base64.getDecoder().decode(edek);
    // String plaintext = "qWruWwlmc7USk6uP41LZBs+gLVfkFWChb+jKivcWK0c=";
    // byte[] plaintextBytes = Base64.getDecoder().decode(plaintext);
    // String response = "{\n" +
    // " \"data\": {\n" +
    // " \"plaintext\": \"" + plaintext + "\"\n" +
    // " }\n" +
    // "}\n";
    // withMockVaultWithSingleResponse(response, vaultKms -> {
    // Assertions.assertThat(vaultKms.decryptEdek(new VaultEdek("kek", edekBytes))).succeedsWithin(Duration.ofSeconds(5))
    // .isInstanceOf(DestroyableRawSecretKey.class)
    // .matches(key -> SecretKeyUtils.same((DestroyableRawSecretKey) key, DestroyableRawSecretKey.takeCopyOf(plaintextBytes, "AES")));
    // });
    // }
    //
    // @Test
    // void testConnectionTimeout() throws NoSuchAlgorithmException {
    // var uri = URI.create("http://test:8080/v1/transit");
    // Duration timeout = Duration.ofMillis(500);
    // VaultKms kms = new VaultKms(uri, "token", timeout, null);
    // SSLContext sslContext = SSLContext.getDefault();
    // HttpClient client = kms.createClient(sslContext);
    // assertThat(client.connectTimeout()).hasValue(timeout);
    // }
    //
    // @Test
    // void testRequestTimeoutConfiguredOnRequests() {
    // var uri = URI.create("http://test:8080/v1/transit");
    // Duration timeout = Duration.ofMillis(500);
    // VaultKms kms = new VaultKms(uri, "token", timeout, null);
    // HttpRequest build = kms.createVaultRequest().uri(uri).build();
    // assertThat(build.timeout()).hasValue(timeout);
    // }
    //
    // static Stream<Arguments> acceptableVaultTransitEnginePaths() {
    // var uri = URI.create("https://localhost:1234");
    // return Stream.of(
    // Arguments.of("basic", uri.resolve("v1/transit"), "/v1/transit/"),
    // Arguments.of("trailing slash", uri.resolve("v1/transit/"), "/v1/transit/"),
    // Arguments.of("single namespace", uri.resolve("v1/ns1/transit"), "/v1/ns1/transit/"),
    // Arguments.of("many namespaces", uri.resolve("v1/ns1/ns2/transit"), "/v1/ns1/ns2/transit/"),
    // Arguments.of("non standard engine name", uri.resolve("v1/mytransit"), "/v1/mytransit/"));
    // }
    //
    // @ParameterizedTest(name = "{0}")
    // @MethodSource("acceptableVaultTransitEnginePaths")
    // void acceptsVaultTransitEnginePaths(String name, URI uri, String expected) {
    // var kms = new VaultKms(uri, "token", Duration.ofMillis(500), null);
    // assertThat(kms.getVaultTransitEngineUri())
    // .extracting(URI::getPath)
    // .isEqualTo(expected);
    //
    // }
    //
    // static Stream<Arguments> unacceptableVaultTransitEnginePaths() {
    // var uri = URI.create("https://localhost:1234");
    // return Stream.of(
    // Arguments.of("no path", uri),
    // Arguments.of("missing path", uri.resolve("/")),
    // Arguments.of("unrecognized API version", uri.resolve("v999/transit")),
    // Arguments.of("missing engine", uri.resolve("v1")),
    // Arguments.of("empty engine", uri.resolve("v1/")));
    // }
    //
    // @ParameterizedTest(name = "{0}")
    // @MethodSource("unacceptableVaultTransitEnginePaths")
    // void detectsUnacceptableVaultTransitEnginePaths(String name, URI uri) {
    // assertThatThrownBy(() -> new VaultKms(uri, "token", Duration.ZERO, null))
    // .isInstanceOf(IllegalArgumentException.class);
    //
    // }

    void withMockAwsWithSingleResponse(String response, Consumer<AwsKmsKms> consumer) {
        withMockAwsWithSingleResponse(response, 200, consumer);
    }

    void withMockAwsWithSingleResponse(String response, int statusCode, Consumer<AwsKmsKms> consumer) {
        HttpHandler handler = statusCode >= 500 ? new ErrorResponse(statusCode) : new StaticResponse(statusCode, response);
        HttpServer httpServer = httpServer(handler);
        try {
            var address = httpServer.getAddress();
            var awsAddress = "http://127.0.0.1:" + address.getPort();
            var config = new Config(URI.create(awsAddress), new InlinePassword("access"), new InlinePassword("secret"), "us-west-2", null);
            var service = new AwsKmsKmsService().buildKms(config);
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

    // private void assertReusesConnectionsOn404(Consumer<AwsKmsKms> consumer) {
    // RemotePortTrackingHandler handler = new RemotePortTrackingHandler();
    // HttpServer httpServer = httpServer(handler);
    // try {
    // InetSocketAddress address = httpServer.getAddress();
    // String vaultAddress = "http://127.0.0.1:" + address.getPort() + "/v1/transit";
    // var config = new Config(URI.create(vaultAddress), new InlinePassword("token"), null);
    // VaultKms service = new VaultKmsService().buildKms(config);
    // for (int i = 0; i < 5; i++) {
    // consumer.accept(service);
    // }
    // assertThat(handler.remotePorts).hasSize(1);
    // }
    // finally {
    // httpServer.stop(0);
    // }
    // }

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
