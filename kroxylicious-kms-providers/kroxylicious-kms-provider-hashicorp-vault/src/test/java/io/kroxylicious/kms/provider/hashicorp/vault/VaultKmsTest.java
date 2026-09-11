/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.tomakehurst.wiremock.WireMockServer;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.testing.kms.SecretKeyUtils;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VaultKmsTest {

    private static final Duration TIMEOUT = Duration.ofMillis(500);
    private static WireMockServer server;
    private VaultKmsService vaultKmsService;
    private VaultKms kms;

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
        var vaultAddress = URI.create(server.baseUrl()).resolve("/v1/transit");
        var config = new Config(vaultAddress, new InlinePassword("token"), null);
        vaultKmsService = new VaultKmsService();
        vaultKmsService.initialize(config);
        kms = vaultKmsService.buildKms();
    }

    @AfterEach
    void afterEach() {
        Optional.ofNullable(vaultKmsService).ifPresent(VaultKmsService::close);
        server.resetAll();
    }

    @Test
    void resolveAlias() {
        // Given
        String response = """
                {
                  "data": {
                    "type": "aes256-gcm96",
                    "deletion_allowed": false,
                    "derived": false,
                    "exportable": false,
                    "allow_plaintext_backup": false,
                    "keys": {
                      "1": 1442851412,
                      "2": 1442851413,
                      "3": 1442851414
                    },
                    "min_decryption_version": 1,
                    "min_encryption_version": 0,
                    "name": "resolved",
                    "latest_version": 3,
                    "supports_encryption": true,
                    "supports_decryption": true,
                    "supports_derivation": true,
                    "supports_signing": false,
                    "imported": false
                  }
                }
                """;

        server.stubFor(get(urlEqualTo("/v1/transit/keys/alias"))
                .willReturn(aResponse().withBody(response)));

        // When / Then
        assertThat(kms.resolveAlias("alias"))
                .succeedsWithin(Duration.ofSeconds(5))
                .extracting(WrappingKey::name, WrappingKey::version)
                .containsExactly("resolved", 3);
    }

    @Test
    void resolveAliasChangesIdentityAfterRotation() {
        // Given - version 1
        String responseV1 = """
                {
                  "data": {
                    "keys": { "1": 1442851412 },
                    "name": "mykey",
                    "latest_version": 1
                  }
                }
                """;
        server.stubFor(get(urlEqualTo("/v1/transit/keys/mykey"))
                .willReturn(aResponse().withBody(responseV1)));

        // When
        var first = kms.resolveAlias("mykey").toCompletableFuture().join();

        // Given - version 2 after rotation
        server.resetAll();
        String responseV2 = """
                {
                  "data": {
                    "keys": { "1": 1442851412, "2": 1442851500 },
                    "name": "mykey",
                    "latest_version": 2
                  }
                }
                """;
        server.stubFor(get(urlEqualTo("/v1/transit/keys/mykey"))
                .willReturn(aResponse().withBody(responseV2)));

        // When
        var second = kms.resolveAlias("mykey").toCompletableFuture().join();

        // Then
        assertThat(first.name()).isEqualTo(second.name());
        assertThat(first).isNotEqualTo(second);
        assertThat(first.version()).isEqualTo(1);
        assertThat(second.version()).isEqualTo(2);
    }

    @Test
    void resolveAliasNotFound() {
        // Given
        server.stubFor(
                get(urlEqualTo("/v1/transit/keys/alias"))
                        .willReturn(aResponse().withStatus(404)));

        // When / Then
        var aliasStage = kms.resolveAlias("alias");
        assertThat(aliasStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownAliasException.class)
                .withMessageContaining("key 'alias' is not found");
    }

    @Test
    void resolveAliasInternalServerError() {
        // Given
        server.stubFor(
                get(urlEqualTo("/v1/transit/keys/alias"))
                        .willReturn(aResponse().withStatus(500)));

        // When / Then
        var aliasStage = kms.resolveAlias("alias");
        assertThat(aliasStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(KmsException.class)
                .withMessageContaining("fail to retrieve key 'alias', HTTP status code 500");
    }

    @Test
    void generateDekPair() {
        // Given
        String plaintext = "dGhlIHF1aWNrIGJyb3duIGZveAo=";
        String ciphertext = "vault:v1:abcdefgh";
        byte[] decoded = Base64.getDecoder().decode(plaintext);
        var expectedKey = DestroyableRawSecretKey.takeCopyOf(decoded, "AES");

        var response = """
                {
                  "data": {
                    "plaintext": "%s",
                    "ciphertext": "%s"
                  }
                }
                """.formatted(plaintext, ciphertext);

        server.stubFor(post(urlEqualTo("/v1/transit/datakey/plaintext/kekref"))
                .willReturn(aResponse().withBody(response)));

        // When / Then
        assertThat(kms.generateDekPair(new WrappingKey("kekref", 1)))
                .succeedsWithin(Duration.ofSeconds(5))
                .satisfies(dekPair -> {
                    assertThat(dekPair)
                            .extracting(DekPair::edek)
                            .isEqualTo(new VaultEdek("kekref", ciphertext.getBytes(StandardCharsets.UTF_8)));
                    assertThat(dekPair)
                            .extracting(DekPair::dek)
                            .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                            .matches(key -> SecretKeyUtils.same(key, expectedKey));
                });
    }

    @Test
    void generateDekPairUsesKeyNameNotVersion() {
        // Given - the datakey URL must contain the plain name, not the version
        String plaintext = "dGhlIHF1aWNrIGJyb3duIGZveAo=";
        String ciphertext = "vault:v2:abcdefgh";

        var response = """
                {
                  "data": {
                    "plaintext": "%s",
                    "ciphertext": "%s"
                  }
                }
                """.formatted(plaintext, ciphertext);

        server.stubFor(post(urlEqualTo("/v1/transit/datakey/plaintext/kekref"))
                .willReturn(aResponse().withBody(response)));

        // When / Then - WrappingKey with version 2, but URL path uses the name only
        assertThat(kms.generateDekPair(new WrappingKey("kekref", 2)))
                .succeedsWithin(Duration.ofSeconds(5))
                .satisfies(dekPair -> {
                    // EDEK still keyed on plain name, not version — wire format unchanged
                    assertThat(dekPair.edek().kekRef()).isEqualTo("kekref");
                });
    }

    @Test
    void generateDekPairWithUriEncodedKeyName() {
        // Given - key name containing a space (requires URI encoding in path)
        String plaintext = "dGhlIHF1aWNrIGJyb3duIGZveAo=";
        String ciphertext = "vault:v1:abcdefgh";

        var response = """
                {
                  "data": {
                    "plaintext": "%s",
                    "ciphertext": "%s"
                  }
                }
                """.formatted(plaintext, ciphertext);

        server.stubFor(post(urlEqualTo("/v1/transit/datakey/plaintext/my+key"))
                .willReturn(aResponse().withBody(response)));

        // When / Then
        assertThat(kms.generateDekPair(new WrappingKey("my key", 1)))
                .succeedsWithin(Duration.ofSeconds(5))
                .satisfies(dekPair -> assertThat(dekPair.edek().kekRef()).isEqualTo("my key"));
    }

    @Test
    void decryptEdek() {
        // Given
        String edek = "dGhlIHF1aWNrIGJyb3duIGZveAo=";
        byte[] edekBytes = Base64.getDecoder().decode(edek);
        String plaintext = "qWruWwlmc7USk6uP41LZBs+gLVfkFWChb+jKivcWK0c=";
        byte[] plaintextBytes = Base64.getDecoder().decode(plaintext);
        var expectedKey = DestroyableRawSecretKey.takeCopyOf(plaintextBytes, "AES");

        var response = """
                {
                  "data": {
                    "plaintext": "%s"
                  }
                }
                """.formatted(plaintext);

        server.stubFor(post(urlEqualTo("/v1/transit/decrypt/kekref"))
                .willReturn(aResponse().withBody(response)));

        // When / Then
        assertThat(kms.decryptEdek(new VaultEdek("kekref", edekBytes)))
                .succeedsWithin(Duration.ofSeconds(5))
                .isInstanceOf(DestroyableRawSecretKey.class)
                .matches(key -> SecretKeyUtils.same((DestroyableRawSecretKey) key, expectedKey));
    }

    @Test
    void appliesConnectionTimeout() {
        var uri = URI.create("http://test:8080/v1/transit");
        var vaultKms = new VaultKms(uri, "token", TIMEOUT, builder -> builder);
        assertThat(vaultKms.getHttpClient().connectTimeout()).hasValue(TIMEOUT);
    }

    @Test
    void appliesRequestTimeoutConfiguredOnRequests() {
        var uri = URI.create("http://test:8080/v1/transit");
        var vaultKms = new VaultKms(uri, "token", TIMEOUT, builder -> builder);
        HttpRequest build = vaultKms.createVaultRequest().uri(uri).build();
        assertThat(build.timeout()).hasValue(TIMEOUT);
    }

    static Stream<Arguments> acceptableVaultTransitEnginePaths() {
        var uri = URI.create("https://localhost:1234");
        return Stream.of(
                Arguments.of("basic", uri.resolve("v1/transit"), "/v1/transit/"),
                Arguments.of("trailing slash", uri.resolve("v1/transit/"), "/v1/transit/"),
                Arguments.of("single namespace", uri.resolve("v1/ns1/transit"), "/v1/ns1/transit/"),
                Arguments.of("many namespaces", uri.resolve("v1/ns1/ns2/transit"), "/v1/ns1/ns2/transit/"),
                Arguments.of("non standard engine name", uri.resolve("v1/mytransit"), "/v1/mytransit/"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("acceptableVaultTransitEnginePaths")
    void acceptsVaultTransitEnginePaths(String name, URI uri, String expected) {
        var vaultKms = new VaultKms(uri, "token", TIMEOUT, builder -> builder);
        assertThat(vaultKms.getVaultTransitEngineUri())
                .extracting(URI::getPath)
                .isEqualTo(expected);

    }

    static Stream<Arguments> unacceptableVaultTransitEnginePaths() {
        var uri = URI.create("https://localhost:1234");
        return Stream.of(
                Arguments.of("no path", uri),
                Arguments.of("missing path", uri.resolve("/")),
                Arguments.of("unrecognized API version", uri.resolve("v999/transit")),
                Arguments.of("missing engine", uri.resolve("v1")),
                Arguments.of("empty engine", uri.resolve("v1/")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unacceptableVaultTransitEnginePaths")
    void detectsUnacceptableVaultTransitEnginePaths(String name, URI uri) {
        assertThatThrownBy(() -> new VaultKms(uri, "token", Duration.ZERO, builder -> builder))
                .isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    void decodeJsonFailure() {
        TypeReference<VaultResponse<VaultResponse.ReadKeyData>> typeRef = new TypeReference<>() {
        };
        byte[] invalidBytes = { 1, 2, 3 };
        assertThatThrownBy(() -> VaultKms.decodeJson(typeRef, invalidBytes))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Failed to decode Vault response as JSON");
    }

}
