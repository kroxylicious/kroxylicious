/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;

import io.kroxylicious.kms.provider.azure.MalformedResponseBodyException;
import io.kroxylicious.kms.provider.azure.UnexpectedHttpStatusCodeException;
import io.kroxylicious.kms.provider.azure.WrappingKey;
import io.kroxylicious.kms.provider.azure.auth.BearerToken;
import io.kroxylicious.kms.provider.azure.auth.BearerTokenService;
import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import edu.umd.cs.findbugs.annotations.NonNull;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class KeyVaultClientTest {

    private static final BearerToken BEARER_TOKEN = new BearerToken("my-token", Instant.MIN, Instant.MAX);
    private static final String KEY_NAME = "my-key";

    private static final byte[] DEK_BYTES = { 4, 5, 6 };
    private static final String DEK_BASE64_URL = "BAUG";

    private static final byte[] EDEK_BYTES = { 1, 2, 3 };
    private static final String EDEK_BASE64_URL = "AQID";
    private static final byte[] EMPTY = {};
    private static final String KEY_VERSION = "keyversion";
    private static final String VAULT_NAME = "default";

    @Mock
    private BearerTokenService service;

    private static WireMockServer server;

    @BeforeAll
    static void initMockServer() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
    }

    @BeforeEach
    void beforeEach() {
        server.resetAll();
    }

    @AfterAll
    static void shutdownMockServer() {
        server.shutdown();
    }

    @Test
    void getKey() {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));
        givenMockGetKeyResponse("""
                {
                  "key": {
                    "kid": "https://myvault.vault.azure.net/keys/CreateSoftKeyTest/78deebed173b48e48f55abf87ed4cf71",
                    "kty": "RSA",
                    "key_ops": [
                      "wrapKey",
                      "unwrapKey"
                    ]
                  },
                  "attributes": {
                    "enabled": true
                  }
                }
                """, "my-key");
        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {
            // when
            CompletionStage<GetKeyResponse> key = keyVaultClient.getKey(VAULT_NAME, KEY_NAME);
            // then
            assertThat(key.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(10)).satisfies(response -> {
                assertThat(response).isNotNull();
                assertThat(response.key()).isNotNull();
                assertThat(response.key().keyType()).isEqualTo("RSA");
                assertThat(response.key().keyId()).isEqualTo("https://myvault.vault.azure.net/keys/CreateSoftKeyTest/78deebed173b48e48f55abf87ed4cf71");
                assertThat(response.key().keyOperations()).isEqualTo(List.of("wrapKey", "unwrapKey"));
                assertThat(response.attributes()).isNotNull();
                assertThat(response.attributes().enabled()).isTrue();
            });
            assertThat(server.getAllServeEvents())
                    .singleElement()
                    .extracting(ServeEvent::getRequest)
                    .satisfies(response -> {
                        assertThat(response.getMethod()).isEqualTo(RequestMethod.GET);
                        assertThat(response.getUrl()).isEqualTo("/keys/my-key?api-version=7.4");
                        assertThat(response.getHeader("Authorization")).isEqualTo("Bearer my-token");
                    });
        }
    }

    private static String getBaseUri() {
        return server.baseUrl();
    }

    @CsvSource({ "301", "302", "303", "400", "401", "404", "500", "201" })
    @ParameterizedTest
    void getKeyRespondsNon200(int code) {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));
        server.stubFor(getKeyMapping(KEY_NAME).willReturn(aResponse().withStatus(code)));
        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {

            // when
            CompletionStage<GetKeyResponse> key = keyVaultClient.getKey(VAULT_NAME, KEY_NAME);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ofSeconds(10)).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOfSatisfying(UnexpectedHttpStatusCodeException.class, e -> {
                        assertThat(e.getStatusCode()).isEqualTo(code);
                    });
        }
    }

    @Test
    void getKeyRespondsWithMalformedJson() {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));

        givenMockGetKeyResponse("""
                1 ! ! not very good JSON, is it?
                """, "my-key");
        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {
            // when
            CompletionStage<GetKeyResponse> key = keyVaultClient.getKey(VAULT_NAME, KEY_NAME);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ofSeconds(10)).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOf(MalformedResponseBodyException.class).withMessage("failed to parse getKey body for key '" + KEY_NAME + "'");
        }
    }

    @Test
    void getKeyRespondsWithNullJson() {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));
        givenMockGetKeyResponse("""
                null
                """, "my-key");
        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {
            // when
            CompletionStage<GetKeyResponse> key = keyVaultClient.getKey(VAULT_NAME, KEY_NAME);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ofSeconds(10)).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOf(MalformedResponseBodyException.class).withMessage("getKeyResponse is null for key '" + KEY_NAME + "'");
        }
    }

    @Test
    void getKeyGetBearerTokenFails() {
        // given
        KmsException exception = new KmsException("failed to get token");
        givenMockEntraBearerFuture(CompletableFuture.failedFuture(exception));
        new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
        try (KeyVaultClient keyVaultClient = getKeyVaultClient("http://localhost:8080")) {
            // when
            CompletionStage<GetKeyResponse> key = keyVaultClient.getKey(VAULT_NAME, KEY_NAME);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isSameAs(exception);
        }
    }

    @Test
    void wrap() {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));
        givenMockWrapKeyResponse("""
                {
                  "kid": "https://myvault.vault.azure.net/keys/sdktestkey/0698c2156c1a4e1da5b6bab6f6422fd6",
                  "value": "%s"
                }
                """.formatted(EDEK_BASE64_URL), "my-key", KEY_VERSION);

        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.wrap(wrappingKey, DEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(10)).satisfies(response -> {
                assertThat(response).isNotNull();
                assertThat(response).containsExactly(EDEK_BYTES);
            });
            assertThat(server.getAllServeEvents())
                    .singleElement()
                    .extracting(ServeEvent::getRequest)
                    .satisfies(request -> {
                        assertThat(request.getMethod()).isEqualTo(RequestMethod.POST);
                        assertThat(request.getUrl()).isEqualTo("/keys/my-key/keyversion/wrapkey?api-version=7.4");
                        assertThat(request.getHeader("Content-type")).isEqualTo("application/json");
                        assertThat(request.getHeader("Authorization")).isEqualTo("Bearer my-token");
                        assertThat(request.getBodyAsString()).isEqualTo("""
                                {"alg":"%s","value":"%s"}""".formatted("RSA-OAEP-256", DEK_BASE64_URL));
                    });
        }
    }

    @Test
    void unwrap() {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));
        givenMockUnwrapKeyResponse("""
                {
                  "kid": "https://myvault.vault.azure.net/keys/sdktestkey/0698c2156c1a4e1da5b6bab6f6422fd6",
                  "value": "%s"
                }
                """.formatted(DEK_BASE64_URL), "my-key", KEY_VERSION);
        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.unwrap(wrappingKey, EDEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(10)).satisfies(response -> {
                assertThat(response).isNotNull();
                assertThat(response).containsExactly(DEK_BYTES);
            });
            assertThat(server.getAllServeEvents())
                    .singleElement()
                    .extracting(ServeEvent::getRequest)
                    .satisfies(request -> {
                        assertThat(request.getMethod()).isEqualTo(RequestMethod.POST);
                        assertThat(request.getUrl()).isEqualTo("/keys/my-key/keyversion/unwrapkey?api-version=7.4");
                        assertThat(request.getHeader("Content-type")).isEqualTo("application/json");
                        assertThat(request.getHeader("Authorization")).isEqualTo("Bearer my-token");
                        assertThat(request.getBodyAsString()).isEqualTo("""
                                {"alg":"%s","value":"%s"}""".formatted("RSA-OAEP-256", EDEK_BASE64_URL));
                    });
        }
    }

    @Test
    void wrapFailsIfInputBytesEmpty() {
        // given

        new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
        try (KeyVaultClient keyVaultClient = getKeyVaultClient("http://localhost:8080")) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.wrap(wrappingKey, EMPTY);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOf(KmsException.class).withMessage("value length is zero for operation wrapkey");
        }
    }

    @Test
    void unwrapFailsIfInputBytesEmpty() {
        // given
        new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
        try (KeyVaultClient keyVaultClient = getKeyVaultClient("http://localhost:8080")) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.unwrap(wrappingKey, EMPTY);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOf(KmsException.class).withMessage("value length is zero for operation unwrapkey");
        }
    }

    @Test
    void wrapGetBearerTokenFails() {
        // given
        KmsException failedToGetToken = new KmsException("failed to get token");
        givenMockEntraBearerFuture(CompletableFuture.failedFuture(failedToGetToken));
        new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
        try (KeyVaultClient keyVaultClient = getKeyVaultClient("http://localhost:8080")) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.wrap(wrappingKey, DEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isSameAs(failedToGetToken);
        }
    }

    @Test
    void unwrapGetBearerTokenFails() {
        // given
        KmsException failedToGetToken = new KmsException("failed to get token");
        givenMockEntraBearerFuture(CompletableFuture.failedFuture(failedToGetToken));
        new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
        try (KeyVaultClient keyVaultClient = getKeyVaultClient("http://localhost:8080")) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.unwrap(wrappingKey, DEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isSameAs(failedToGetToken);
        }
    }

    @CsvSource({ "301", "302", "303", "400", "401", "404", "500", "201" })
    @ParameterizedTest
    void wrapNon200Response(int responseCode) {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));
        server.stubFor(wrapKeyMapping(KEY_NAME, KEY_VERSION).willReturn(aResponse().withStatus(responseCode)));
        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.wrap(wrappingKey, DEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ofSeconds(10)).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOfSatisfying(UnexpectedHttpStatusCodeException.class, e -> {
                        assertThat(e.getStatusCode()).isEqualTo(responseCode);
                    });
        }
    }

    @CsvSource({ "301", "302", "303", "400", "401", "404", "500", "201" })
    @ParameterizedTest
    void unwrapNon200Response(int responseCode) {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));
        server.stubFor(unwrapKeyMapping(KEY_NAME, KEY_VERSION).willReturn(aResponse().withStatus(responseCode)));
        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.unwrap(wrappingKey, EDEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ofSeconds(10)).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOfSatisfying(UnexpectedHttpStatusCodeException.class, e -> {
                        assertThat(e.getStatusCode()).isEqualTo(responseCode);
                    });
        }
    }

    @Test
    void wrapMalformedJson() {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));
        givenMockWrapKeyResponse("""
                 not such a good json!
                """, KEY_NAME, KEY_VERSION);
        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.wrap(wrappingKey, DEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ofSeconds(10)).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOf(MalformedResponseBodyException.class)
                    .withMessage("failed to parse body for key '" + KEY_NAME + "' for operation wrapkey");
        }
    }

    @Test
    void unwrapMalformedJson() {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));
        givenMockUnwrapKeyResponse("""
                 not such a good json!
                """, KEY_NAME, KEY_VERSION);
        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.unwrap(wrappingKey, EDEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ofSeconds(10)).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOf(MalformedResponseBodyException.class)
                    .withMessage("failed to parse body for key '" + KEY_NAME + "' for operation unwrapkey");
        }
    }

    @Test
    void wrapNullJson() {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));
        givenMockWrapKeyResponse("""
                null
                """, KEY_NAME, KEY_VERSION);
        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.wrap(wrappingKey, DEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ofSeconds(10)).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOf(MalformedResponseBodyException.class)
                    .withMessage("response body null for key '" + KEY_NAME + "' for operation wrapkey");
        }
    }

    @Test
    void unwrapNullJson() {
        // given
        givenMockEntraBearerFuture(completedFuture(BEARER_TOKEN));
        givenMockUnwrapKeyResponse("""
                null
                """, KEY_NAME, KEY_VERSION);
        try (KeyVaultClient keyVaultClient = getKeyVaultClient(getBaseUri())) {
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SupportedKeyType.RSA, "myvault");
            // when
            CompletionStage<byte[]> key = keyVaultClient.unwrap(wrappingKey, EDEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ofSeconds(10)).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOf(MalformedResponseBodyException.class)
                    .withMessage("response body null for key '" + KEY_NAME + "' for operation unwrapkey");
        }
    }

    private static void givenMockGetKeyResponse(String getKeyResponse, String keyName) {
        MappingBuilder mapping = getKeyMapping(keyName);
        mockJsonResponse(getKeyResponse, mapping);
    }

    private static void givenMockWrapKeyResponse(String responseBody, String keyName, String keyVersion) {
        MappingBuilder mapping = wrapKeyMapping(keyName, keyVersion);
        mockJsonResponse(responseBody, mapping);
    }

    private static void givenMockUnwrapKeyResponse(String responseBody, String keyName, String keyVersion) {
        MappingBuilder mapping = unwrapKeyMapping(keyName, keyVersion);
        mockJsonResponse(responseBody, mapping);
    }

    private static void mockJsonResponse(String getKeyResponse, MappingBuilder mapping) {
        byte[] responseBytes = getKeyResponse.getBytes(StandardCharsets.UTF_8);
        server.stubFor(mapping.willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(responseBytes)));
    }

    private static MappingBuilder getKeyMapping(String keyName) {
        return get(urlPathEqualTo("/keys/" + keyName));
    }

    private static MappingBuilder wrapKeyMapping(String keyName, String keyVersion) {
        return post(urlPathEqualTo("/keys/" + keyName + "/" + keyVersion + "/wrapkey"));
    }

    private static MappingBuilder unwrapKeyMapping(String keyName, String keyVersion) {
        return post(urlPathEqualTo("/keys/" + keyName + "/" + keyVersion + "/unwrapkey"));
    }

    private void givenMockEntraBearerFuture(CompletableFuture<BearerToken> future) {
        Mockito.when(service.getBearerToken()).thenReturn(future);
    }

    @NonNull
    private KeyVaultClient getKeyVaultClient(String address) {
        EntraIdentityConfig arbitraryEntraConfig = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
        URI baseUri = URI.create(address);
        Integer port = baseUri.getPort() == -1 ? null : baseUri.getPort();
        // note that we rely on `baseUri.getHost()` being localhost, so that LocalhostSubdomainResolverProvider can resolve ${VAULT_NAME}.localhost to localhost.
        return new KeyVaultClient(service,
                new AzureKeyVaultConfig(arbitraryEntraConfig, VAULT_NAME, baseUri.getHost(), baseUri.getScheme(), port, null));
    }

}