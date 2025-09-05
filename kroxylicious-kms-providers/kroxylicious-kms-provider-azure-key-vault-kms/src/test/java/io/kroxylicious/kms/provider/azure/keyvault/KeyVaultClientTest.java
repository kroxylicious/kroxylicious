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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.kms.httpmock.MockServer;
import io.kroxylicious.kms.httpmock.MockServer.Header;
import io.kroxylicious.kms.httpmock.MockServer.Request;
import io.kroxylicious.kms.provider.azure.MalformedResponseBodyException;
import io.kroxylicious.kms.provider.azure.UnexpectedHttpStatusCodeException;
import io.kroxylicious.kms.provider.azure.WrappingKey;
import io.kroxylicious.kms.provider.azure.auth.BearerToken;
import io.kroxylicious.kms.provider.azure.auth.BearerTokenService;
import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class KeyVaultClientTest {

    public static final BearerToken BEARER_TOKEN = new BearerToken("my-token", Instant.MIN, Instant.MAX);
    public static final String KEY_NAME = "my-key";

    public static final byte[] DEK_BYTES = { 4, 5, 6 };
    public static final String DEK_BASE64_URL = "BAUG";

    public static final byte[] EDEK_BYTES = { 1, 2, 3 };
    public static final String EDEK_BASE64_URL = "AQID";
    public static final byte[] EMPTY = {};
    @Mock
    private BearerTokenService service;

    @Test
    void getKey() {
        // given
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] responseBytes = """
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
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            // when
            CompletionStage<GetKeyResponse> key = keyVaultClient.getKey(KEY_NAME);
            // then
            assertThat(key.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(10)).satisfies(response -> {
                assertThat(response).isNotNull();
                assertThat(response.supportedKeyType()).contains(SupportedKeyType.RSA);
                assertThat(response.key()).isNotNull();
                assertThat(response.key().keyType()).isEqualTo("RSA");
                assertThat(response.key().keyId()).isEqualTo("https://myvault.vault.azure.net/keys/CreateSoftKeyTest/78deebed173b48e48f55abf87ed4cf71");
                assertThat(response.key().keyOperations()).isEqualTo(List.of("wrapKey", "unwrapKey"));
                assertThat(response.attributes()).isNotNull();
                assertThat(response.attributes().enabled()).isTrue();
            });
            Request request = httpServer.singleReceivedRequest();
            assertThat(request.method()).isEqualTo("GET");
            assertThat(request.requestUri()).isEqualTo(URI.create("/keys/my-key?api-version=7.4"));
            assertThat(request.headers()).containsAll(List.of(new Header("Authorization", "Bearer my-token")));
        }
    }

    @CsvSource({ "301", "302", "303", "400", "401", "404", "500", "201" })
    @ParameterizedTest
    void getKeyRespondsNon200(int code) {
        // given
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.sendResponseHeaders(code, 0);
            exchange.getResponseBody().close();
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            // when
            CompletionStage<GetKeyResponse> key = keyVaultClient.getKey(KEY_NAME);
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
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] responseBytes = """
                    1 ! ! not very good JSON, is it?
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            // when
            CompletionStage<GetKeyResponse> key = keyVaultClient.getKey(KEY_NAME);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ofSeconds(10)).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOf(MalformedResponseBodyException.class).withMessage("failed to parse getKey body for key '" + KEY_NAME + "'");
        }
    }

    @Test
    void getKeyRespondsWithNullJson() {
        // given
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] responseBytes = """
                    null
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            // when
            CompletionStage<GetKeyResponse> key = keyVaultClient.getKey(KEY_NAME);
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
        Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.failedFuture(exception));
        EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
        KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, "localhost:8080", null));
        // when
        CompletionStage<GetKeyResponse> key = keyVaultClient.getKey(KEY_NAME);
        // then
        assertThat(key.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat()
                .isInstanceOf(ExecutionException.class).havingCause()
                .isSameAs(exception);
    }

    @Test
    void wrap() {
        // given
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] responseBytes = """
                    {
                      "kid": "https://myvault.vault.azure.net/keys/sdktestkey/0698c2156c1a4e1da5b6bab6f6422fd6",
                      "value": "%s"
                    }
                    """.formatted(EDEK_BASE64_URL).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
            // when
            CompletionStage<byte[]> key = keyVaultClient.wrap(wrappingKey, DEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(10)).satisfies(response -> {
                assertThat(response).isNotNull();
                assertThat(response).containsExactly(EDEK_BYTES);
            });
            Request request = httpServer.singleReceivedRequest();
            assertThat(request.method()).isEqualTo("POST");
            assertThat(request.requestUri()).isEqualTo(URI.create("/keys/my-key/keyversion/wrapkey?api-version=7.4"));
            assertThat(request.headers()).containsAll(List.of(new Header("Content-type", "application/json"), new Header("Authorization", "Bearer my-token")));
            assertThat(request.body()).isEqualTo("""
                    {"alg":"%s","value":"%s"}""".formatted("RSA-OAEP-256", DEK_BASE64_URL));
        }
    }

    @Test
    void unwrap() {
        // given
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] responseBytes = """
                    {
                      "kid": "https://myvault.vault.azure.net/keys/sdktestkey/0698c2156c1a4e1da5b6bab6f6422fd6",
                      "value": "%s"
                    }
                    """.formatted(DEK_BASE64_URL).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
            // when
            CompletionStage<byte[]> key = keyVaultClient.unwrap(wrappingKey, EDEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(10)).satisfies(response -> {
                assertThat(response).isNotNull();
                assertThat(response).containsExactly(DEK_BYTES);
            });
            Request request = httpServer.singleReceivedRequest();
            assertThat(request.method()).isEqualTo("POST");
            assertThat(request.requestUri()).isEqualTo(URI.create("/keys/my-key/keyversion/unwrapkey?api-version=7.4"));
            assertThat(request.headers()).containsAll(List.of(new Header("Content-type", "application/json"), new Header("Authorization", "Bearer my-token")));
            assertThat(request.body()).isEqualTo("""
                    {"alg":"%s","value":"%s"}""".formatted("RSA-OAEP-256", EDEK_BASE64_URL));
        }
    }

    @Test
    void wrapFailsIfInputBytesEmpty() {
        // given
        EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
        KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, "localhost:8080", null));
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
        // when
        CompletionStage<byte[]> key = keyVaultClient.wrap(wrappingKey, EMPTY);
        // then
        assertThat(key.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat()
                .isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(KmsException.class).withMessage("value length is zero for operation wrapkey");
    }

    @Test
    void unwrapFailsIfInputBytesEmpty() {
        // given
        EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
        KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, "localhost:8080", null));
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
        // when
        CompletionStage<byte[]> key = keyVaultClient.unwrap(wrappingKey, EMPTY);
        // then
        assertThat(key.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat()
                .isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(KmsException.class).withMessage("value length is zero for operation unwrapkey");
    }

    @Test
    void wrapGetBearerTokenFails() {
        // given
        KmsException failedToGetToken = new KmsException("failed to get token");
        Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.failedFuture(failedToGetToken));
        EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
        KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, "localhost:8080", null));
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
        // when
        CompletionStage<byte[]> key = keyVaultClient.wrap(wrappingKey, DEK_BYTES);
        // then
        assertThat(key.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat()
                .isInstanceOf(ExecutionException.class).havingCause()
                .isSameAs(failedToGetToken);
    }

    @Test
    void unwrapGetBearerTokenFails() {
        // given
        KmsException failedToGetToken = new KmsException("failed to get token");
        Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.failedFuture(failedToGetToken));
        EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
        KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, "localhost:8080", null));
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
        // when
        CompletionStage<byte[]> key = keyVaultClient.unwrap(wrappingKey, DEK_BYTES);
        // then
        assertThat(key.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat()
                .isInstanceOf(ExecutionException.class).havingCause()
                .isSameAs(failedToGetToken);
    }

    @CsvSource({ "301", "302", "303", "400", "401", "404", "500", "201" })
    @ParameterizedTest
    void wrapNon200Response(int responseCode) {
        // given
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.sendResponseHeaders(responseCode, 0);
            exchange.getResponseBody().close();
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
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
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.sendResponseHeaders(responseCode, 0);
            exchange.getResponseBody().close();
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
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
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] responseBytes = """
                    not such a good json!
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
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
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] responseBytes = """
                    not such a good json!
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
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
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] responseBytes = """
                    null
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
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
        try (MockServer httpServer = MockServer.getHttpServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] responseBytes = """
                    null
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
        })) {
            Mockito.when(service.getBearerToken()).thenReturn(CompletableFuture.completedFuture(BEARER_TOKEN));
            EntraIdentityConfig arbitrary = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            KeyVaultClient keyVaultClient = new KeyVaultClient(service, new AzureKeyVaultConfig(arbitrary, httpServer.address(), null));
            WrappingKey wrappingKey = new WrappingKey(KEY_NAME, "keyversion", SupportedKeyType.RSA);
            // when
            CompletionStage<byte[]> key = keyVaultClient.unwrap(wrappingKey, EDEK_BYTES);
            // then
            assertThat(key.toCompletableFuture()).failsWithin(Duration.ofSeconds(10)).withThrowableThat()
                    .isInstanceOf(ExecutionException.class).havingCause()
                    .isInstanceOf(MalformedResponseBodyException.class)
                    .withMessage("response body null for key '" + KEY_NAME + "' for operation unwrapkey");
        }
    }

}