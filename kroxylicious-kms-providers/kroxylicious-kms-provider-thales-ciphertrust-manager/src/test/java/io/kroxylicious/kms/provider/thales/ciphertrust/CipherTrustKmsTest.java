/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust;

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

import io.kroxylicious.kms.provider.thales.ciphertrust.config.Config;
import io.kroxylicious.kms.provider.thales.ciphertrust.config.UserCredentials;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.testing.kms.SecretKeyUtils;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link CipherTrustKms}.
 */
class CipherTrustKmsTest {

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    private static final String MOCK_JWT_TOKEN = "mock-jwt-token";

    private static WireMockServer server;
    private CipherTrustKmsService service;
    private CipherTrustKms kms;

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
        // Stub authentication endpoint
        stubAuthEndpoint();

        var config = new Config(
                URI.create(server.baseUrl()),
                new UserCredentials("testuser", new InlinePassword("testpass")),
                null);

        service = new CipherTrustKmsService();
        service.initialize(config);
        kms = (CipherTrustKms) service.buildKms();
    }

    @AfterEach
    void afterEach() {
        Optional.ofNullable(service).ifPresent(CipherTrustKmsService::close);
        server.resetAll();
    }

    @Test
    void resolveAlias() {
        // Stub key lookup endpoint
        String keyId = "test-key-id-12345";
        String alias = "test-alias";
        long version = 0L;
        String response = """
                {
                    "id": "%s",
                    "name": "%s",
                    "algorithm": "AES",
                    "version": %d
                }
                """.formatted(keyId, alias, version);

        server.stubFor(
                get(urlPathMatching("/api/v1/vault/keys2/[^/]+"))
                        .withQueryParam("type", equalTo("name"))
                        .withHeader("Authorization", equalTo("Bearer " + MOCK_JWT_TOKEN))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));

        var aliasStage = kms.resolveAlias(alias);
        // resolveAlias returns WrappingKey with name and version
        assertThat(aliasStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .extracting(WrappingKey::name, WrappingKey::version)
                .containsExactly(alias, version);
    }

    @Test
    void resolveAliasNotFound() {
        String alias = "non-existent-alias";

        server.stubFor(
                get(urlPathMatching("/api/v1/vault/keys2/[^/]+"))
                        .withQueryParam("type", equalTo("name"))
                        .withHeader("Authorization", equalTo("Bearer " + MOCK_JWT_TOKEN))
                        .willReturn(aResponse()
                                .withStatus(404)));

        var aliasStage = kms.resolveAlias(alias);
        assertThat(aliasStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownAliasException.class)
                .withMessageContaining(alias);
    }

    @Test
    void resolveAliasInternalServerError() {
        String alias = "test-alias";

        server.stubFor(
                get(urlPathMatching("/api/v1/vault/keys2/[^/]+"))
                        .withQueryParam("type", equalTo("name"))
                        .withHeader("Authorization", equalTo("Bearer " + MOCK_JWT_TOKEN))
                        .willReturn(aResponse().withStatus(500)));

        var aliasStage = kms.resolveAlias(alias);
        assertThat(aliasStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .havingRootCause()
                .isInstanceOf(KmsException.class)
                .withMessageContaining("HTTP 500");
    }

    @Test
    void generateDekPair() {
        String kekName = "test-kek-name";
        long kekVersion = 1L;
        WrappingKey kekRef = new WrappingKey(kekName, kekVersion);
        String kekId = "test-kek-id";

        // Stub random bytes generation
        byte[] randomBytes = new byte[32];
        for (int i = 0; i < randomBytes.length; i++) {
            randomBytes[i] = (byte) i;
        }
        String randomResponse = """
                {
                    "bytes": "%s"
                }
                """.formatted(BASE64_ENCODER.encodeToString(randomBytes));

        server.stubFor(
                get(urlPathMatching("/api/v1/vault/random.*"))
                        .withQueryParam("bytes", equalTo("32"))
                        .withHeader("Authorization", equalTo("Bearer " + MOCK_JWT_TOKEN))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(randomResponse)));

        // Stub encryption endpoint
        byte[] ciphertext = "encrypted-dek".getBytes(StandardCharsets.UTF_8);
        byte[] tag = "auth-tag".getBytes(StandardCharsets.UTF_8);
        byte[] iv = "init-vector".getBytes(StandardCharsets.UTF_8);
        String encryptResponse = """
                {
                    "id": "%s",
                    "ciphertext": "%s",
                    "tag": "%s",
                    "version": 1,
                    "mode": "gcm",
                    "iv": "%s"
                }
                """.formatted(
                kekId,
                BASE64_ENCODER.encodeToString(ciphertext),
                BASE64_ENCODER.encodeToString(tag),
                BASE64_ENCODER.encodeToString(iv));

        server.stubFor(
                post(urlPathEqualTo("/api/v1/crypto/encrypt"))
                        .withHeader("Authorization", equalTo("Bearer " + MOCK_JWT_TOKEN))
                        .withRequestBody(matchingJsonPath("$.id", equalTo(kekName)))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(encryptResponse)));

        var expectedKey = DestroyableRawSecretKey.takeCopyOf(randomBytes, "AES");
        var expectedEdek = new CipherTrustEdek(kekId, ciphertext, tag, 1, "gcm", iv);

        var dekStage = kms.generateDekPair(kekRef);
        assertThat(dekStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .satisfies(dekPair -> {
                    assertThat(dekPair)
                            .extracting(DekPair::edek)
                            .isEqualTo(expectedEdek);

                    assertThat(dekPair)
                            .extracting(DekPair::dek)
                            .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                            .matches(key -> SecretKeyUtils.same(key, expectedKey));
                });
    }

    @Test
    void generateDekPairAfterRotationUsesNewKeyVersion() {
        String keyName = "rotation-test-key";
        WrappingKey initialKekRef = new WrappingKey(keyName, 0L);
        WrappingKey rotatedKekRef = new WrappingKey(keyName, 1L);
        String initialKeyId = "key-id-v0";
        String rotatedKeyId = "key-id-v1";

        // Stub random bytes generation
        byte[] randomBytes = new byte[32];
        for (int i = 0; i < randomBytes.length; i++) {
            randomBytes[i] = (byte) i;
        }
        String randomResponse = """
                {
                    "bytes": "%s"
                }
                """.formatted(BASE64_ENCODER.encodeToString(randomBytes));

        server.stubFor(
                get(urlPathMatching("/api/v1/vault/random.*"))
                        .withQueryParam("bytes", equalTo("32"))
                        .withHeader("Authorization", equalTo("Bearer " + MOCK_JWT_TOKEN))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(randomResponse)));

        // Stub encryption with initial key (version 0)
        byte[] ciphertext = "encrypted-dek".getBytes(StandardCharsets.UTF_8);
        byte[] tag = "auth-tag".getBytes(StandardCharsets.UTF_8);
        byte[] iv = "init-vector".getBytes(StandardCharsets.UTF_8);

        server.stubFor(
                post(urlPathEqualTo("/api/v1/crypto/encrypt"))
                        .withHeader("Authorization", equalTo("Bearer " + MOCK_JWT_TOKEN))
                        .withRequestBody(matchingJsonPath("$.id", equalTo(keyName)))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody("""
                                        {
                                            "id": "%s",
                                            "ciphertext": "%s",
                                            "tag": "%s",
                                            "version": 0,
                                            "mode": "gcm",
                                            "iv": "%s"
                                        }
                                        """.formatted(
                                        initialKeyId,
                                        BASE64_ENCODER.encodeToString(ciphertext),
                                        BASE64_ENCODER.encodeToString(tag),
                                        BASE64_ENCODER.encodeToString(iv)))));

        // Generate first DEK pair with version 0
        var firstDekPair = kms.generateDekPair(initialKekRef)
                .toCompletableFuture().join();

        assertThat(firstDekPair.edek().version()).isZero();
        assertThat(firstDekPair.edek().id()).isEqualTo(initialKeyId);

        // Simulate rotation: CTM now returns rotated key (version 1, new ID)
        server.stubFor(
                post(urlPathEqualTo("/api/v1/crypto/encrypt"))
                        .withHeader("Authorization", equalTo("Bearer " + MOCK_JWT_TOKEN))
                        .withRequestBody(matchingJsonPath("$.id", equalTo(keyName)))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody("""
                                        {
                                            "id": "%s",
                                            "ciphertext": "%s",
                                            "tag": "%s",
                                            "version": 1,
                                            "mode": "gcm",
                                            "iv": "%s"
                                        }
                                        """.formatted(
                                        rotatedKeyId,
                                        BASE64_ENCODER.encodeToString(ciphertext),
                                        BASE64_ENCODER.encodeToString(tag),
                                        BASE64_ENCODER.encodeToString(iv)))));

        // Generate second DEK pair with rotated version (same name, different version)
        var secondDekPair = kms.generateDekPair(rotatedKekRef)
                .toCompletableFuture().join();

        // Assert the EDEK uses the NEW key version after rotation
        assertThat(secondDekPair.edek().version())
                .isEqualTo(1)
                .isGreaterThan(firstDekPair.edek().version());
        assertThat(secondDekPair.edek().id()).isEqualTo(rotatedKeyId);

        // Verify WrappingKeys are different (enables cache invalidation)
        assertThat(initialKekRef).isNotEqualTo(rotatedKekRef);
    }

    @Test
    void resolveAliasIncludesVersionForCacheInvalidation() {
        String keyName = "versioned-key";
        String keyId = "test-key-id";

        // Stub initial key lookup (version 0)
        server.stubFor(
                get(urlPathMatching("/api/v1/vault/keys2/[^/]+"))
                        .withQueryParam("type", equalTo("name"))
                        .withHeader("Authorization", equalTo("Bearer " + MOCK_JWT_TOKEN))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody("""
                                        {
                                            "id": "%s",
                                            "name": "%s",
                                            "algorithm": "AES",
                                            "version": 0
                                        }
                                        """.formatted(keyId, keyName))));

        var firstRef = kms.resolveAlias(keyName).toCompletableFuture().join();

        assertThat(firstRef.name()).isEqualTo(keyName);
        assertThat(firstRef.version()).isZero();

        // Simulate rotation - same key name, different version
        server.stubFor(
                get(urlPathMatching("/api/v1/vault/keys2/[^/]+"))
                        .withQueryParam("type", equalTo("name"))
                        .withHeader("Authorization", equalTo("Bearer " + MOCK_JWT_TOKEN))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody("""
                                        {
                                            "id": "%s-v1",
                                            "name": "%s",
                                            "algorithm": "AES",
                                            "version": 1
                                        }
                                        """.formatted(keyId, keyName))));

        var secondRef = kms.resolveAlias(keyName).toCompletableFuture().join();

        assertThat(secondRef.name()).isEqualTo(keyName);
        assertThat(secondRef.version()).isEqualTo(1L);

        // Different versions means different cache keys - enables cache invalidation
        assertThat(firstRef).isNotEqualTo(secondRef);
    }

    @Test
    void decryptEdek() {
        String kekId = "test-kek-id";
        byte[] plaintext = "plaintext-dek-bytes".getBytes(StandardCharsets.UTF_8);
        byte[] ciphertext = "encrypted-dek".getBytes(StandardCharsets.UTF_8);
        byte[] tag = "auth-tag".getBytes(StandardCharsets.UTF_8);
        byte[] iv = "init-vector".getBytes(StandardCharsets.UTF_8);

        var edek = new CipherTrustEdek(kekId, ciphertext, tag, 1, "gcm", iv);

        String decryptResponse = """
                {
                    "plaintext": "%s"
                }
                """.formatted(BASE64_ENCODER.encodeToString(plaintext));

        server.stubFor(
                post(urlPathEqualTo("/api/v1/crypto/decrypt"))
                        .withHeader("Authorization", equalTo("Bearer " + MOCK_JWT_TOKEN))
                        .withRequestBody(matchingJsonPath("$.id", equalTo(kekId)))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(decryptResponse)));

        var expectedKey = DestroyableRawSecretKey.takeCopyOf(plaintext, "AES");

        var keyStage = kms.decryptEdek(edek);
        assertThat(keyStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                .matches(key -> SecretKeyUtils.same(key, expectedKey));
    }

    @Test
    void authenticationOccursExactlyOnce() {
        // Given - stub key operations
        String keyId = "test-key-id";
        String alias = "test-alias";
        stubKeyLookup(alias, keyId);

        assertThat(kms.resolveAlias(alias))
                .succeedsWithin(Duration.ofSeconds(5));

        // When - perform second KMS operation
        assertThat(kms.resolveAlias(alias))
                .succeedsWithin(Duration.ofSeconds(5));

        // Then - verify password authentication occurred exactly once (on first operation)
        server.verify(1, postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(containing("\"grant_type\":\"password\"")));

        // And no refresh token calls were made (token was cached and reused)
        server.verify(0, postRequestedFor(urlPathEqualTo("/api/v1/auth/tokens/"))
                .withRequestBody(containing("\"grant_type\":\"refresh_token\"")));
    }

    @Test
    void resolveAliasWithSpecialCharactersInName() {
        // Given - Create a key with spaces in the name
        String aliasWithSpecialChars = "my test key";
        String keyId = "test-key-id";
        stubKeyLookup(aliasWithSpecialChars, keyId);

        // When - Resolve the alias (should URI-encode the name)
        WrappingKey result = kms.resolveAlias(aliasWithSpecialChars)
                .toCompletableFuture()
                .join();

        // Then - Should successfully resolve
        assertThat(result.name()).isEqualTo(aliasWithSpecialChars);
        assertThat(result.version()).isEqualTo(0L);
    }

    private void stubKeyLookup(String alias, String keyId) {
        String response = """
                {
                    "id": "%s",
                    "name": "%s",
                    "algorithm": "AES",
                    "version": 0
                }
                """.formatted(keyId, alias);

        server.stubFor(
                get(urlPathMatching("/api/v1/vault/keys2/[^/]+"))
                        .withQueryParam("type", equalTo("name"))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));
    }

    private void stubAuthEndpoint() {
        String authResponse = """
                {
                    "jwt": "%s",
                    "duration": 300,
                    "refresh_token": "mock-refresh-token"
                }
                """.formatted(MOCK_JWT_TOKEN);

        server.stubFor(
                post(urlPathEqualTo("/api/v1/auth/tokens/"))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(authResponse)));
    }

}
