/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.thales.ciphertrust.auth.BearerTokenService;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.DecryptRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.DecryptResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.EncryptRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.EncryptResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.GetKeyResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.RandomResponse;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Implementation of {@link Kms} backed by Thales CipherTrust Manager.
 * <p>
 * Implements envelope encryption using CTM's primitive cryptographic operations:
 * </p>
 * <ul>
 *   <li>Generate random DEK bytes via {@code /api/v1/vault/random}</li>
 *   <li>Encrypt DEK with KEK via {@code /api/v1/crypto/encrypt}</li>
 *   <li>Decrypt EDEK via {@code /api/v1/crypto/decrypt}</li>
 * </ul>
 *
 * <h2>CipherTrust Manager Key Model and Rotation</h2>
 * <p>
 * CTM uses a versioned key model where:
 * </p>
 * <ul>
 *   <li><strong>Key Name</strong>: User-facing identifier (stable across rotations)</li>
 *   <li><strong>Key ID</strong>: Internal UUID identifier (changes on each rotation)</li>
 *   <li><strong>Key Version</strong>: Incremental counter (0, 1, 2, ...)</li>
 * </ul>
 * <p>
 * When a key is rotated, CTM creates a new key object with a new ID and incremented version,
 * but keeps the same name. The old key continues to exist with its original ID and the same name.
 * </p>
 *
 * <h2>Design: WrappingKey with Version for Cache Invalidation</h2>
 * <p>
 * To support key rotation correctly with caching layers, this implementation uses {@link WrappingKey}
 * as the reference type {@code K} in {@code Kms<K, E>}. WrappingKey contains both the key name and version:
 * </p>
 * <ul>
 *   <li>{@link #resolveAlias(String)} returns a {@link WrappingKey} containing the alias name and current version</li>
 *   <li>{@link #generateDekPair(WrappingKey)} receives a WrappingKey and uses the name for CTM's encrypt endpoint</li>
 *   <li>CTM's {@code /api/v1/crypto/encrypt} accepts names (using heuristics to distinguish from UUIDs)
 *       and automatically encrypts with the current (highest version) key</li>
 * </ul>
 * <p>
 * When a key is rotated, the version changes, causing the WrappingKey to be different.
 * This triggers cache misses in caching layers (like {@code EncryptionDekCache}), ensuring
 * newly generated DEKs use the rotated key within the alias cache refresh period (~8-10 minutes)
 * rather than waiting for the DEK cache to expire (~1-2 hours).
 * </p>
 * <p>
 * For decryption, the EDEK contains the specific key ID and version used during encryption,
 * ensuring the correct key version is used to decrypt.
 * </p>
 */
public class CipherTrustKms implements Kms<WrappingKey, CipherTrustEdek> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CipherTrustKms.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String AES_KEY_ALGO = "AES";
    private static final int DEK_SIZE_BYTES = 32; // 256-bit AES key
    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final TypeReference<RandomResponse> RANDOM_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<EncryptResponse> ENCRYPT_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<DecryptResponse> DECRYPT_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<GetKeyResponse> GET_KEY_RESPONSE_TYPE_REF = new TypeReference<>() {
    };

    private final URI endpointUrl;
    private final BearerTokenService tokenService;
    private final HttpClient client;

    /**
     * Create a CipherTrust Manager KMS instance.
     *
     * @param endpointUrl base URL of CipherTrust Manager instance
     * @param tokenService bearer token service for authentication
     * @param timeout HTTP request timeout
     * @param tlsConfigurator TLS configuration for HTTP client
     */
    public CipherTrustKms(URI endpointUrl,
                          BearerTokenService tokenService,
                          Duration timeout,
                          UnaryOperator<HttpClient.Builder> tlsConfigurator) {
        Objects.requireNonNull(endpointUrl, "endpointUrl cannot be null");
        Objects.requireNonNull(tokenService, "tokenService cannot be null");
        Objects.requireNonNull(timeout, "timeout cannot be null");
        Objects.requireNonNull(tlsConfigurator, "tlsConfigurator cannot be null");

        this.endpointUrl = endpointUrl;
        this.tokenService = tokenService;
        this.client = createClient(timeout, tlsConfigurator);
    }

    private HttpClient createClient(Duration timeout, UnaryOperator<HttpClient.Builder> tlsConfigurator) {
        return tlsConfigurator.apply(HttpClient.newBuilder())
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(timeout)
                .build();
    }

    private byte[] getBody(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(obj);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to serialize request", e);
        }
    }

    private <T> T decodeJson(TypeReference<T> typeRef, byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, typeRef);
        }
        catch (IOException e) {
            String responseBody = new String(bytes, StandardCharsets.UTF_8);
            LOGGER.atWarn()
                    .setCause(e)
                    .addKeyValue("responseBody", responseBody)
                    .log("failed to parse response");
            throw new UncheckedIOException("Failed to parse response", e);
        }
    }

    private CompletionStage<HttpRequest> createPostRequest(Object requestObj, URI uri) {
        byte[] body = getBody(requestObj);
        String bodyStr = new String(body, StandardCharsets.UTF_8);

        return tokenService.getBearerToken()
                .thenApply(token -> buildPostJsonRequest(uri, token.token(), bodyStr));
    }

    private CompletionStage<HttpRequest> createGetRequest(URI uri) {
        return tokenService.getBearerToken()
                .thenApply(token -> buildGetRequest(uri, token.token()));
    }

    private CompletionStage<HttpResponse<byte[]>> executeRequest(HttpRequest request,
                                                                 String operation,
                                                                 @Nullable Supplier<KmsException> notFoundExceptionSupplier) {
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> checkResponseStatus(response, operation, notFoundExceptionSupplier));
    }

    private <T> CompletionStage<T> sendAsync(HttpRequest request,
                                             TypeReference<T> valueTypeRef,
                                             String operation,
                                             @Nullable Supplier<KmsException> notFoundExceptionSupplier) {
        return executeRequest(request, operation, notFoundExceptionSupplier)
                .thenApply(HttpResponse::body)
                .thenApply(body -> decodeJson(valueTypeRef, body));
    }

    @Override
    public CompletionStage<DekPair<CipherTrustEdek>> generateDekPair(WrappingKey kekRef) {
        LOGGER.atDebug()
                .addKeyValue("kekName", kekRef.name())
                .log("generating DEK pair");

        // Use the key name from WrappingKey for the CTM API call.
        // CTM's /v1/crypto/encrypt automatically uses the current (highest version) key for that name.
        // This ensures DEKs are always encrypted with the latest rotated key.

        // Step 1: Generate random DEK bytes
        return generateRandomBytes(DEK_SIZE_BYTES)
                // Step 2: Encrypt DEK with KEK (CTM resolves name to current key version)
                .thenCompose(plaintextDek -> encryptDek(kekRef.name(), plaintextDek)
                        .thenApply(edek -> {
                            // Step 3: Create DekPair
                            SecretKey secretKey = DestroyableRawSecretKey.takeOwnershipOf(plaintextDek, AES_KEY_ALGO);
                            // Log the version actually used by CTM's encrypt endpoint.
                            // Note: this may differ from kekRef.version() due to rotation between
                            // alias resolution and encryption - CTM always uses the latest version.
                            LOGGER.atDebug()
                                    .addKeyValue("kekName", kekRef.name())
                                    .addKeyValue("kekVersion", edek.version())
                                    .log("DEK pair generated successfully");
                            return new DekPair<>(edek, secretKey);
                        }));
    }

    private CompletionStage<byte[]> generateRandomBytes(int numBytes) {
        URI randomUri = endpointUrl.resolve("/api/v1/vault/random?bytes=" + numBytes);

        return createGetRequest(randomUri)
                .thenCompose(request -> sendAsync(request, RANDOM_RESPONSE_TYPE_REF, "random generation", null))
                .thenApply(RandomResponse::bytes);
    }

    private CompletionStage<CipherTrustEdek> encryptDek(String kekRef, byte[] plaintextDek) {
        URI encryptUri = endpointUrl.resolve("/api/v1/crypto/encrypt");
        EncryptRequest encryptRequest = new EncryptRequest(kekRef, plaintextDek, "name");

        return createPostRequest(encryptRequest, encryptUri)
                .thenCompose(request -> sendAsync(request, ENCRYPT_RESPONSE_TYPE_REF, "encryption",
                        () -> new UnknownKeyException("key '%s' not found".formatted(kekRef))))
                .thenApply(encryptResponse -> new CipherTrustEdek(
                        encryptResponse.id(),
                        encryptResponse.ciphertext(),
                        encryptResponse.tag(),
                        encryptResponse.version(),
                        encryptResponse.mode(),
                        encryptResponse.iv()));
    }

    @Override
    public CompletionStage<SecretKey> decryptEdek(CipherTrustEdek edek) {
        LOGGER.atDebug()
                .addKeyValue("kekRef", edek.id())
                .log("decrypting EDEK");

        URI decryptUri = endpointUrl.resolve("/api/v1/crypto/decrypt");
        DecryptRequest decryptRequest = new DecryptRequest(
                edek.ciphertext(),
                edek.tag(),
                edek.id(),
                edek.version(),
                edek.mode(),
                edek.iv());

        String keyId = edek.id();
        return createPostRequest(decryptRequest, decryptUri)
                .thenCompose(request -> executeRequest(request, "decryption",
                        () -> new UnknownKeyException("key '%s' not found".formatted(keyId))))
                .thenApply(HttpResponse::body)
                .thenApply(bytes -> {
                    DecryptResponse response = decodeJson(DECRYPT_RESPONSE_TYPE_REF, bytes);
                    // Zero out the response body bytes to avoid leaving plaintext in memory
                    Arrays.fill(bytes, (byte) 0);
                    return response;
                })
                .thenApply(decryptResponse -> {
                    byte[] plaintextDek = decryptResponse.plaintext();
                    LOGGER.atDebug()
                            .addKeyValue("kekRef", keyId)
                            .log("EDEK decrypted successfully");
                    return DestroyableRawSecretKey.takeOwnershipOf(plaintextDek, AES_KEY_ALGO);
                });
    }

    @Override
    public CompletionStage<WrappingKey> resolveAlias(String alias) {
        LOGGER.atDebug()
                .addKeyValue("alias", alias)
                .log("resolving key alias");

        // Query CTM to get the current key name and version.
        // The version changes on rotation, causing cache invalidation in caching layers.
        URI keysUri = endpointUrl.resolve("/api/v1/vault/keys2/%s?type=name".formatted(encode(alias, UTF_8)));

        return createGetRequest(keysUri)
                .thenCompose(request -> sendAsync(request, GET_KEY_RESPONSE_TYPE_REF, "alias resolution",
                        () -> new UnknownAliasException(alias)))
                .thenApply(keyResponse -> {
                    LOGGER.atDebug()
                            .addKeyValue("alias", alias)
                            .addKeyValue("currentKeyId", keyResponse.id())
                            .addKeyValue("currentVersion", keyResponse.version())
                            .log("Alias resolved with version");

                    // Return name and version - version changes on rotation trigger DEK cache miss
                    return new WrappingKey(alias, keyResponse.version());
                });
    }

    @Override
    public Serde<CipherTrustEdek> edekSerde() {
        return CipherTrustEdekSerde.instance();
    }

    /**
     * Package-private accessor for testing.
     * @return the HTTP client
     */
    @VisibleForTesting
    HttpClient getHttpClient() {
        return client;
    }

    private HttpRequest buildGetRequest(URI uri, String bearerToken) {
        return HttpRequest.newBuilder()
                .uri(uri)
                .header("Authorization", bearerToken(bearerToken))
                .header("Accept", JSON_CONTENT_TYPE)
                .GET()
                .build();
    }

    private HttpRequest buildPostJsonRequest(URI uri, String bearerToken, String jsonBody) {
        return HttpRequest.newBuilder()
                .uri(uri)
                .header("Authorization", bearerToken(bearerToken))
                .header("Content-Type", JSON_CONTENT_TYPE)
                .header("Accept", JSON_CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                .build();
    }

    private static String bearerToken(String bearerToken) {
        return "Bearer " + bearerToken;
    }

    private HttpResponse<byte[]> checkResponseStatus(HttpResponse<byte[]> response,
                                                     String operation,
                                                     @Nullable Supplier<KmsException> notFoundExceptionSupplier) {
        int statusCode = response.statusCode();

        if (statusCode == 404) {
            String body = new String(response.body(), StandardCharsets.UTF_8);
            LOGGER.atWarn()
                    .addKeyValue("operation", operation)
                    .addKeyValue("statusCode", statusCode)
                    .addKeyValue("responseBody", body)
                    .log("resource not found");

            throw notFoundExceptionSupplier != null
                    ? notFoundExceptionSupplier.get()
                    : new KmsException("%s failed: resource not found".formatted(operation));
        }
        else if (statusCode != 200) {
            String body = new String(response.body(), StandardCharsets.UTF_8);
            var logBuilder = LOGGER.atWarn()
                    .addKeyValue("operation", operation)
                    .addKeyValue("statusCode", statusCode);
            if (LOGGER.isDebugEnabled()) {
                logBuilder = logBuilder.addKeyValue("responseBody", body);
            }
            logBuilder.log(LOGGER.isDebugEnabled()
                    ? "KMS operation failed"
                    : "KMS operation failed, increase log level to DEBUG for response body");
            throw new KmsException("%s failed with HTTP %d".formatted(operation, statusCode));
        }

        return response;
    }

}
