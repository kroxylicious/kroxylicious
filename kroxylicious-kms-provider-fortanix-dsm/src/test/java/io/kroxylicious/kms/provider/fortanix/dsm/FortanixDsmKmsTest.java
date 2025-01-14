/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;

import io.kroxylicious.kms.provider.fortanix.dsm.config.ApiKeySessionProviderConfig;
import io.kroxylicious.kms.provider.fortanix.dsm.config.Config;
import io.kroxylicious.kms.provider.fortanix.dsm.model.DecryptResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.EncryptResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.SecurityObjectResponse;
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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link FortanixDsmKms}.  See also io.kroxylicious.kms.service.KmsIT.
 */
class FortanixDsmKmsTest {

    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
    private static final String KEYS_ENDPOINT = "/crypto/v1/keys";
    private static final String KEYS_INFO_ENDPOINT = "/crypto/v1/keys/info";
    private static final String KEYS_EXPORT_ENDPOINT = "/crypto/v1/keys/export";
    private static final String ENCRYPT_ENDPOINT = "/crypto/v1/encrypt";
    private static final String DECRYPT_ENDPOINT = "/crypto/v1/decrypt";
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static WireMockServer server;
    private FortanixDsmKmsService kmsService;
    private FortanixDsmKms kms;
    private Config config;

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

        kmsService = new FortanixDsmKmsService(new StubSessionProviderFactory());
        kmsService.initialize(config);
        kms = kmsService.buildKms();
    }

    @AfterEach
    void afterEach() {
        Optional.ofNullable(kmsService).ifPresent(KmsService::close);
        server.resetAll();
    }

    @Test
    void resolveAlias() {
        var alias = "alias";
        var response = new SecurityObjectResponse("1234abcd-12ab-34cd-56ef-1234567890ab", null, null);

        server.stubFor(post(urlEqualTo(KEYS_INFO_ENDPOINT))
                .withRequestBody(matchingJsonPath("$.name", equalTo(alias)))
                .willReturn(aResponse().withBody(toJson(response))));

        var expectedKeyId = "1234abcd-12ab-34cd-56ef-1234567890ab";

        var aliasStage = kms.resolveAlias(alias);
        assertThat(aliasStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(expectedKeyId);
    }

    @Test
    void resolveAliasNotFound() {

        server.stubFor(post(urlEqualTo(KEYS_INFO_ENDPOINT))
                .willReturn(aResponse().withStatus(404)));

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
                .willReturn(aResponse().withStatus(500)));

        var aliasStage = kms.resolveAlias("alias");
        assertThat(aliasStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(KmsException.class)
                .withMessageContaining("crypto/v1/keys/info, HTTP status code 500");
    }

    @Test
    void unauthorizedResponseInvalidatesSession() {
        var spFactory = mock(SessionProviderFactory.class);
        var sp = mock(SessionProvider.class);
        var session = mock(Session.class);
        when(spFactory.createSessionProvider(any(Config.class), any(HttpClient.class))).thenReturn(sp);
        when(sp.getSession()).thenReturn(CompletableFuture.completedStage(session));
        when(session.authorizationHeader()).thenReturn("auth header");

        try (var service = new FortanixDsmKmsService(spFactory)) {
            service.initialize(config);
            var k = service.buildKms();

            server.stubFor(post(urlEqualTo(KEYS_INFO_ENDPOINT))
                    .willReturn(aResponse().withStatus(403)));

            var aliasStage = k.resolveAlias("alias");
            assertThat(aliasStage)
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .withCauseInstanceOf(KmsException.class)
                    .withMessageContaining("crypto/v1/keys/info, HTTP status code 403");

            verify(session, times(1)).invalidate();
        }
    }

    @Test
    void generateDekPair() {
        var kekRef = "1234abcd-12ab-34cd-56ef-1234567890ab";
        var createTmpKeyResponse = new SecurityObjectResponse(null,
                "SlmFdhHoCP8G4U81WQuZAfVaDumf3xBAzhEDHQc7WjSBpQBAAQICWMwwMN49eIlFggkLLqdgGJtecC3LWFMr1jEq/GiJNo39xOGL6AaN8fIz82fH2JUJIvMxtDRzfBWulitHjC5g9DMRBxWIR0vMW32yIBWuKPAgbiSKmDK9j6L7XGy6oKVBJgMLdQRgOuczriQiKjRuVaQAHvRzx9Fnj6jQPy6jOCkw3hmRs3aBCSmr5WsICgLeBbqHqO7mi/E/JJux6y/I+AGw1RBqAbHGeeIV7zLV+4Krbi46ITDVSptqSzvgn8k7X6BwpnKGPO3rUifP2TEDUB+YpqOct/LGOO0MEBICgZsEUKh19IKwTF05A3D8a6lfe/c=",
                null);
        var exportResponse = new SecurityObjectResponse(null, createTmpKeyResponse.transientKey(), BASE64_DECODER.decode("tGvHSBuXa1CEjFERiS8pjctNlYCbAwzzH3rjLyyaIcE="));
        var encryptResponse = new EncryptResponse(kekRef, BASE64_DECODER.decode("cQeXs0xgAq3fZyo/t8zpa/Y7hdidQuGiAe6o4TEMa45XzZFeP5fw4WgnjhAPuT2T"),
                BASE64_DECODER.decode("aQEy9ja1u5pS8zF3z6U27A=="));

        server.stubFor(post(urlEqualTo(KEYS_ENDPOINT))
                .withRequestBody(matchingJsonPath("$.name", containing("dek-")))
                .withRequestBody(matchingJsonPath("$.transient", equalTo("true")))
                .willReturn(aResponse().withBody(toJson(createTmpKeyResponse))));

        server.stubFor(post(urlEqualTo(KEYS_EXPORT_ENDPOINT))
                .withRequestBody(matchingJsonPath("$.transient_key", equalTo(createTmpKeyResponse.transientKey())))
                .willReturn(aResponse().withBody(toJson(exportResponse))));

        server.stubFor(post(urlEqualTo(ENCRYPT_ENDPOINT))
                .withRequestBody(matchingJsonPath("$.key.kid", equalTo(kekRef)))
                .willReturn(aResponse().withBody(toJson(encryptResponse))));

        var dekPairStage = kms.generateDekPair(kekRef);

        var expectedCipherText = encryptResponse.cipher();
        var expectedDekIv = encryptResponse.iv();

        assertThat(dekPairStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .extracting(DekPair::edek)
                .isEqualTo(new FortanixDsmKmsEdek(kekRef, expectedDekIv, expectedCipherText));

        var plainTextBytes = exportResponse.value();
        var expectedKey = DestroyableRawSecretKey.takeCopyOf(plainTextBytes, "AES");

        assertThat(dekPairStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .extracting(DekPair::dek)
                .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                .matches(key -> SecretKeyUtils.same(key, expectedKey));

    }

    @Test
    void decryptEdek() {

        var kekRef = "kekRef";
        var decryptResponse = new DecryptResponse(kekRef, BASE64_DECODER.decode("tGvHSBuXa1CEjFERiS8pjctNlYCbAwzzH3rjLyyaIcE="));

        server.stubFor(post(urlEqualTo(DECRYPT_ENDPOINT))
                .withRequestBody(matchingJsonPath("$.key.kid", equalTo(kekRef)))
                .willReturn(aResponse().withBody(toJson(decryptResponse))));

        var expectedKey = DestroyableRawSecretKey.takeCopyOf(decryptResponse.plain(), "AES");
        var dekIv = BASE64_DECODER.decode("aQEy9ja1u5pS8zF3z6U27A==");
        var cipher = "unused".getBytes(StandardCharsets.UTF_8);

        var edek = new FortanixDsmKmsEdek(kekRef, dekIv, cipher);
        var dekPairStage = kms.decryptEdek(edek);
        assertThat(dekPairStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                .matches(key -> SecretKeyUtils.same(key, expectedKey));
    }

    private byte[] toJson(Object credentials) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(credentials);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class StubSessionProviderFactory implements SessionProviderFactory {
        @NonNull
        @Override
        public SessionProvider createSessionProvider(@NonNull Config config, HttpClient client) {
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

                @Override
                public void invalidate() {
                    // not required.
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
    }
}
