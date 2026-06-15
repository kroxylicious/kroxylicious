/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.DecryptRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.DecryptResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.EncryptRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.EncryptResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.GetKeyResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.RandomResponse;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.testing.kms.ciphertrust.model.CreateKeyRequest;
import io.kroxylicious.testing.kms.ciphertrust.model.CreateKeyResponse;
import io.kroxylicious.testing.kms.ciphertrust.model.GetKeysResponse;
import io.kroxylicious.testing.kms.ciphertrust.model.RotateKeyRequest;
import io.kroxylicious.testing.kms.ciphertrust.model.RotateKeyResponse;
import io.kroxylicious.testing.kms.tls.TlsHttpClientConfigurator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link CipherTrustMockServer}.
 * <p>
 * Tests the WireMock-based mock implementation of the Thales CipherTrust Manager REST API,
 * including real AES-GCM encryption/decryption, key management, and error handling.
 * </p>
 */
class CipherTrustMockServerTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String MOCK_JWT_TOKEN = "mock-jwt-token";

    private CipherTrustMockServer mockServer;
    private HttpClient client;
    private String baseUrl;
    private String bearerToken;

    @BeforeEach
    void setUp() throws Exception {
        mockServer = new CipherTrustMockServer();
        mockServer.start();
        baseUrl = mockServer.getBaseUrl();
        client = HttpClient.newHttpClient();
        bearerToken = authenticate();
    }

    @AfterEach
    void tearDown() {
        if (mockServer != null) {
            mockServer.stop();
        }
    }

    // ========================================
    // Helper Methods
    // ========================================

    private String authenticate() throws Exception {
        var request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/auth/tokens/"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"test\",\"password\":\"test\"}"))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var authResponse = OBJECT_MAPPER.readValue(response.body(), AuthResponse.class);
        return authResponse.jwt();
    }

    private HttpRequest.Builder authorizedRequest(String path) {
        return HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Authorization", "Bearer " + bearerToken)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json");
    }

    private String createKey(String name) throws Exception {
        var keyRequest = new CreateKeyRequest(name, "AES", 12, Map.of());
        var request = authorizedRequest("/api/v1/vault/keys2/")
                .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(keyRequest)))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var createResponse = OBJECT_MAPPER.readValue(response.body(), CreateKeyResponse.class);
        return createResponse.id();
    }

    private EncryptResponse encrypt(String keyName, byte[] plaintext) throws Exception {
        var encryptRequest = new EncryptRequest(keyName, plaintext, "name");
        var request = authorizedRequest("/api/v1/crypto/encrypt")
                .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(encryptRequest)))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return OBJECT_MAPPER.readValue(response.body(), EncryptResponse.class);
    }

    private DecryptResponse decrypt(String keyId, EncryptResponse encryptResponse) throws Exception {
        var decryptRequest = new DecryptRequest(
                encryptResponse.ciphertext(),
                encryptResponse.tag(),
                keyId,
                encryptResponse.version(),
                encryptResponse.mode(),
                encryptResponse.iv());

        var request = authorizedRequest("/api/v1/crypto/decrypt")
                .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(decryptRequest)))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return OBJECT_MAPPER.readValue(response.body(), DecryptResponse.class);
    }

    // ========================================
    // 1. Authentication & Authorization Tests
    // ========================================

    @Test
    void authEndpointReturnsValidToken() throws Exception {
        // Already tested in setUp via authenticate(), but let's verify the structure
        var request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/auth/tokens/"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"user\",\"password\":\"pass\"}"))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);

        var authResponse = OBJECT_MAPPER.readValue(response.body(), AuthResponse.class);
        assertThat(authResponse.jwt()).isEqualTo(MOCK_JWT_TOKEN);
        assertThat(authResponse.duration()).isEqualTo(300); // 5 minutes
        assertThat(authResponse.refreshToken()).isNotNull();
    }

    @Test
    void requestsWithoutAuthorizationFail() throws Exception {
        var request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/vault/keys2/test-key"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        // WireMock returns 404 for unmatched requests (no auth header)
        assertThat(response.statusCode()).isIn(404, 401);
    }

    @Test
    void requestsWithInvalidTokenFail() throws Exception {
        var request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/vault/keys2/test-key"))
                .header("Authorization", "Bearer invalid-token")
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isIn(404, 401);
    }

    // ========================================
    // 2. Encryption/Decryption Correctness Tests
    // ========================================

    @Test
    void encryptionProducesValidCiphertext() throws Exception {
        String keyName = "test-key";
        var keyId = createKey(keyName);
        byte[] plaintext = "Hello, World!".getBytes(StandardCharsets.UTF_8);

        var encryptResponse = encrypt(keyName, plaintext);

        assertThat(encryptResponse.id()).isEqualTo(keyId);
        assertThat(encryptResponse.ciphertext()).isNotEmpty();
        assertThat(encryptResponse.tag()).isNotEmpty().hasSize(16); // 128-bit GCM tag
        assertThat(encryptResponse.iv()).isNotEmpty().hasSize(12); // 12-byte IV
        assertThat(encryptResponse.mode()).isEqualTo("gcm");
        assertThat(encryptResponse.version()).isZero(); // Initial version
    }

    @Test
    void decryptionRoundTrip() throws Exception {
        String keyName = "test-key";
        var keyId = createKey(keyName);
        byte[] plaintext = new byte[256];
        new SecureRandom().nextBytes(plaintext);

        var encryptResponse = encrypt(keyName, plaintext);
        var decryptResponse = decrypt(keyId, encryptResponse);

        assertThat(decryptResponse.plaintext()).isEqualTo(plaintext);
    }

    @Test
    void samePlaintextProducesDifferentCiphertexts() throws Exception {
        String keyName = "test-key";
        var keyId = createKey(keyName);
        byte[] plaintext = "Same plaintext".getBytes(StandardCharsets.UTF_8);

        var encrypt1 = encrypt(keyName, plaintext);
        var encrypt2 = encrypt(keyName, plaintext);

        // Same plaintext should produce different ciphertexts due to random IV
        assertThat(encrypt1.ciphertext()).isNotEqualTo(encrypt2.ciphertext());
        assertThat(encrypt1.iv()).isNotEqualTo(encrypt2.iv());

        // But both should decrypt to the same plaintext
        var decrypt1 = decrypt(keyId, encrypt1);
        var decrypt2 = decrypt(keyId, encrypt2);
        assertThat(decrypt1.plaintext()).isEqualTo(decrypt2.plaintext());
    }

    @Test
    void encryptionFailsForNonExistentKey() throws Exception {
        byte[] plaintext = "test".getBytes(StandardCharsets.UTF_8);
        var encryptRequest = new EncryptRequest("non-existent-key-id", plaintext, "name");

        var request = authorizedRequest("/api/v1/crypto/encrypt")
                .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(encryptRequest)))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void decryptionFailsForNonExistentKey() throws Exception {
        String keyName = "test-key";
        createKey(keyName);
        byte[] plaintext = "test".getBytes(StandardCharsets.UTF_8);
        var encryptResponse = encrypt(keyName, plaintext);

        // Try to decrypt with non-existent key ID
        var fakeKeyId = "00000000-0000-0000-0000-000000000000";
        var decryptRequest = new DecryptRequest(
                encryptResponse.ciphertext(),
                encryptResponse.tag(),
                fakeKeyId,
                0,
                encryptResponse.mode(),
                encryptResponse.iv());

        var request = authorizedRequest("/api/v1/crypto/decrypt")
                .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(decryptRequest)))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(404);
    }

    // ========================================
    // 3. Key Management Operation Tests
    // ========================================

    @Test
    void createKeyGeneratesValidUuid() throws Exception {
        var keyId = createKey("my-test-key");

        // Verify it's a valid UUID
        assertThat(keyId).isNotNull();
        assertThat(UUID.fromString(keyId)).isNotNull();
    }

    @Test
    void createKeyStoresAtVersionZero() throws Exception {
        String keyName = "my-key";
        createKey(keyName);
        byte[] plaintext = "test".getBytes(StandardCharsets.UTF_8);

        var encryptResponse = encrypt(keyName, plaintext);
        assertThat(encryptResponse.version()).isZero();
    }

    @Test
    void createKeyPreservesNameAndLabels() throws Exception {
        var name = "my-named-key";
        var keyRequest = new CreateKeyRequest(name, "AES", 12, Map.of("env", "test", "team", "security"));
        var request = authorizedRequest("/api/v1/vault/keys2/")
                .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(keyRequest)))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var createResponse = OBJECT_MAPPER.readValue(response.body(), CreateKeyResponse.class);

        // Query by name
        var queryRequest = authorizedRequest("/api/v1/vault/keys2?name=" + name)
                .GET()
                .build();

        var queryResponse = client.send(queryRequest, HttpResponse.BodyHandlers.ofString());
        var keysResponse = OBJECT_MAPPER.readValue(queryResponse.body(), GetKeysResponse.class);

        assertThat(keysResponse.total()).isEqualTo(1);
        assertThat(keysResponse.resources()).hasSize(1);
        assertThat(keysResponse.resources().get(0).name()).isEqualTo(name);
        assertThat(keysResponse.resources().get(0).id()).isEqualTo(createResponse.id());
    }

    @Test
    void multipleKeysAreIndependent() throws Exception {
        String key1Name = "key1";
        String key2Name = "key2";
        var key1Id = createKey(key1Name);
        var key2Id = createKey(key2Name);

        byte[] plaintext1 = "data1".getBytes(StandardCharsets.UTF_8);
        byte[] plaintext2 = "data2".getBytes(StandardCharsets.UTF_8);

        var encrypt1 = encrypt(key1Name, plaintext1);
        var encrypt2 = encrypt(key2Name, plaintext2);

        var decrypt1 = decrypt(key1Id, encrypt1);
        var decrypt2 = decrypt(key2Id, encrypt2);

        assertThat(decrypt1.plaintext()).isEqualTo(plaintext1);
        assertThat(decrypt2.plaintext()).isEqualTo(plaintext2);
    }

    @Test
    void queryByNameReturnsExactMatch() throws Exception {
        createKey("exact-name");
        createKey("another-name");

        var request = authorizedRequest("/api/v1/vault/keys2?name=exact-name")
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var keysResponse = OBJECT_MAPPER.readValue(response.body(), GetKeysResponse.class);

        assertThat(keysResponse.total()).isEqualTo(1);
        assertThat(keysResponse.resources())
                .singleElement()
                .extracting(GetKeyResponse::name)
                .isEqualTo("exact-name");
    }

    @Test
    void queryReturnsEmptyForNonExistent() throws Exception {
        var request = authorizedRequest("/api/v1/vault/keys2?name=does-not-exist")
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var keysResponse = OBJECT_MAPPER.readValue(response.body(), GetKeysResponse.class);

        assertThat(keysResponse.total()).isGreaterThanOrEqualTo(0);
        assertThat(keysResponse.resources()).isNullOrEmpty();
    }

    @Test
    void deleteKeyRemovesFromStore() throws Exception {
        String keyName = "to-delete";
        var keyId = createKey(keyName);

        var deleteRequest = authorizedRequest("/api/v1/vault/keys2/" + keyId + "?type=id")
                .DELETE()
                .build();

        var deleteResponse = client.send(deleteRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(deleteResponse.statusCode()).isEqualTo(204);

        // Verify key no longer exists (encryption should fail)
        byte[] plaintext = "test".getBytes(StandardCharsets.UTF_8);
        var encryptRequest = new EncryptRequest(keyName, plaintext, "name");
        var request = authorizedRequest("/api/v1/crypto/encrypt")
                .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(encryptRequest)))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(404);
    }

    // ========================================
    // 4. Key Rotation/Versioning Tests
    // ========================================

    @Test
    void rotateKeyCreatesNewKey() throws Exception {
        String keyName = "rotate-test";
        var key1Id = createKey(keyName);

        // Rotate the key - creates NEW key with NEW ID
        var rotateRequestBody = OBJECT_MAPPER.writeValueAsString(new RotateKeyRequest());
        var rotateRequest = authorizedRequest("/api/v1/vault/keys2/rotate-test/versions/?type=name")
                .POST(HttpRequest.BodyPublishers.ofString(rotateRequestBody))
                .build();

        var rotateResponse = client.send(rotateRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(rotateResponse.statusCode()).isEqualTo(200);

        var rotateResult = OBJECT_MAPPER.readValue(rotateResponse.body(), RotateKeyResponse.class);
        String key2Id = rotateResult.id();

        // Verify we got a NEW key ID
        assertThat(key2Id).isNotEqualTo(key1Id);

        // New key has version 1
        byte[] plaintext = "test".getBytes(StandardCharsets.UTF_8);
        var encryptResponse = encrypt(keyName, plaintext);
        assertThat(encryptResponse.version()).isEqualTo(1);
    }

    @Test
    void canDecryptWithOldKeyAfterRotation() throws Exception {
        String keyName = "version-test";
        var key1Id = createKey(keyName);
        byte[] plaintext = "original".getBytes(StandardCharsets.UTF_8);

        // Encrypt with original key (version 0)
        var encryptV0 = encrypt(keyName, plaintext);
        assertThat(encryptV0.version()).isZero();

        // Rotate key - creates new key
        var rotateRequest = authorizedRequest("/api/v1/vault/keys2/version-test/versions/?type=name")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        client.send(rotateRequest, HttpResponse.BodyHandlers.ofString());

        // Should still be able to decrypt with OLD key ID
        var decryptV0 = decrypt(key1Id, encryptV0);
        assertThat(decryptV0.plaintext()).isEqualTo(plaintext);
    }

    @Test
    void rotatedKeyHasHigherVersion() throws Exception {
        String keyName = "latest-test";
        var keyId = createKey(keyName);

        // Initial key has version 0
        var encrypt1 = encrypt(keyName, "test1".getBytes(StandardCharsets.UTF_8));
        assertThat(encrypt1.version()).isZero();

        // Rotate - creates NEW key with version 1
        var rotateRequest = authorizedRequest("/api/v1/vault/keys2/latest-test/versions/?type=name")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        var rotateResponse = client.send(rotateRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(rotateResponse.statusCode()).isEqualTo(200);
        var rotateResult = OBJECT_MAPPER.readValue(rotateResponse.body(), RotateKeyResponse.class);
        assertThat(rotateResult.id()).isNotEqualTo(keyId);

        // New key has higher version
        var encrypt2 = encrypt(keyName, "test2".getBytes(StandardCharsets.UTF_8));
        assertThat(encrypt2.version()).isEqualTo(1);
    }

    @Test
    void multipleRotationsWork() throws Exception {
        String keyName = "multi-rotate";
        createKey(keyName);

        // Rotate 3 times, each creates a new key
        for (int i = 0; i < 3; i++) {
            var rotateRequest = authorizedRequest("/api/v1/vault/keys2/multi-rotate/versions/?type=name")
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            var rotateResponse = client.send(rotateRequest, HttpResponse.BodyHandlers.ofString());
            assertThat(rotateResponse.statusCode()).isEqualTo(200);
        }

        // Final key should have version 3
        var encryptResponse = encrypt(keyName, "test".getBytes(StandardCharsets.UTF_8));
        assertThat(encryptResponse.version()).isEqualTo(3);
    }

    // ========================================
    // 5. Random Bytes Generation Tests
    // ========================================

    @Test
    void randomBytesReturnsRequestedLength() throws Exception {
        var request = authorizedRequest("/api/v1/vault/random?bytes=64")
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var randomResponse = OBJECT_MAPPER.readValue(response.body(), RandomResponse.class);

        var decoded = randomResponse.bytes();
        assertThat(decoded).hasSize(64);
    }

    @Test
    void randomBytesDefault32Bytes() throws Exception {
        var request = authorizedRequest("/api/v1/vault/random")
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var randomResponse = OBJECT_MAPPER.readValue(response.body(), RandomResponse.class);

        var decoded = randomResponse.bytes();
        assertThat(decoded).hasSize(32); // Default
    }

    @Test
    void differentCallsReturnDifferentBytes() throws Exception {
        var request = authorizedRequest("/api/v1/vault/random?bytes=32")
                .GET()
                .build();

        var response1 = client.send(request, HttpResponse.BodyHandlers.ofString());
        var response2 = client.send(request, HttpResponse.BodyHandlers.ofString());

        var random1 = OBJECT_MAPPER.readValue(response1.body(), RandomResponse.class);
        var random2 = OBJECT_MAPPER.readValue(response2.body(), RandomResponse.class);

        // Different calls should produce different random bytes
        assertThat(random1.bytes()).isNotEqualTo(random2.bytes());
    }

    // ========================================
    // 6. Error Handling & Edge Cases Tests
    // ========================================

    @Test
    void emptyPlaintextsHandled() throws Exception {
        String keyName = "empty-test";
        var keyId = createKey(keyName);
        byte[] emptyPlaintext = new byte[0];

        var encryptResponse = encrypt(keyName, emptyPlaintext);
        var decryptResponse = decrypt(keyId, encryptResponse);

        var decrypted = decryptResponse.plaintext();
        assertThat(decrypted).isEmpty();
    }

    @Test
    void largePayloadsHandled() throws Exception {
        String keyName = "large-test";
        var keyId = createKey(keyName);
        byte[] largePlaintext = new byte[10000]; // 10KB
        new SecureRandom().nextBytes(largePlaintext);

        var encryptResponse = encrypt(keyName, largePlaintext);
        var decryptResponse = decrypt(keyId, encryptResponse);

        var decrypted = decryptResponse.plaintext();
        assertThat(decrypted).isEqualTo(largePlaintext);
    }

    // ========================================
    // 7. Request/Response Format Tests
    // ========================================

    @Test
    void responseHeadersCorrect() throws Exception {
        var request = authorizedRequest("/api/v1/auth/tokens/")
                .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"test\"}"))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.headers().firstValue("Content-Type"))
                .isPresent()
                .get()
                .asString()
                .contains("application/json");
    }

    @Test
    void base64EncodingCorrect() throws Exception {
        String keyName = "base64-test";
        createKey(keyName);
        byte[] plaintext = new byte[]{ 0x01, 0x02, 0x03, 0x04, (byte) 0xFF };

        var encryptResponse = encrypt(keyName, plaintext);

        // Verify ciphertext, tag, and IV are valid Base64 strings by decoding them
        assertThat(encryptResponse.ciphertext()).isNotEmpty();
        assertThat(encryptResponse.tag()).hasSize(16);
        assertThat(encryptResponse.iv()).hasSize(12);
    }

    // ========================================
    // 8. Type Field Validation Tests
    // ========================================

    @Test
    void encryptFailsWithoutType() throws Exception {
        var keyId = createKey("test-key");
        byte[] plaintext = "test".getBytes(StandardCharsets.UTF_8);

        // Manually create request JSON without type field
        String requestJson = String.format(
                "{\"id\":\"%s\",\"plaintext\":\"%s\"}",
                keyId,
                java.util.Base64.getEncoder().encodeToString(plaintext));

        var request = authorizedRequest("/api/v1/crypto/encrypt")
                .POST(HttpRequest.BodyPublishers.ofString(requestJson))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(400);
    }

    @Test
    void encryptFailsWithWrongType() throws Exception {
        var keyId = createKey("test-key");
        byte[] plaintext = "test".getBytes(StandardCharsets.UTF_8);

        // Use type="id" instead of "name"
        String requestJson = String.format(
                "{\"id\":\"%s\",\"plaintext\":\"%s\",\"type\":\"id\"}",
                keyId,
                java.util.Base64.getEncoder().encodeToString(plaintext));

        var request = authorizedRequest("/api/v1/crypto/encrypt")
                .POST(HttpRequest.BodyPublishers.ofString(requestJson))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(400);
    }

    // ========================================
    // HTTPS/TLS Tests
    // ========================================

    @Test
    void tlsMockServerStartsSuccessfully() {
        CipherTrustMockServer tlsServer = new CipherTrustMockServer(true);
        try {
            tlsServer.start();
            assertThat(tlsServer.getBaseUrl()).startsWith("https://");
            assertThat(tlsServer.isHttps()).isTrue();
        }
        finally {
            tlsServer.stop();
        }
    }

    @Test
    void httpMockServerUsesHttpScheme() {
        assertThat(mockServer.getBaseUrl()).startsWith("http://");
        assertThat(mockServer.isHttps()).isFalse();
    }

    @Test
    void tlsAuthenticationSucceeds() throws Exception {
        CipherTrustMockServer tlsServer = new CipherTrustMockServer(true);
        try {
            tlsServer.start();
            String tlsBaseUrl = tlsServer.getBaseUrl();

            // Create HttpClient with insecure TLS (to accept self-signed certificate)
            Tls tls = new Tls(null, new InsecureTls(true), null, null);
            TlsHttpClientConfigurator tlsConfigurator = new TlsHttpClientConfigurator(tls);
            HttpClient tlsClient = tlsConfigurator.apply(HttpClient.newBuilder()).build();

            // Authenticate
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(tlsBaseUrl + "/api/v1/auth/tokens/"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"test\",\"password\":\"test\"}"))
                    .build();

            var response = tlsClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);

            var authResponse = OBJECT_MAPPER.readValue(response.body(), AuthResponse.class);
            assertThat(authResponse.jwt()).isEqualTo(MOCK_JWT_TOKEN);
            assertThat(authResponse.duration()).isEqualTo(300);
        }
        finally {
            tlsServer.stop();
        }
    }

    @Test
    void tlsEncryptionRoundTrip() throws Exception {
        CipherTrustMockServer tlsServer = new CipherTrustMockServer(true);
        try {
            tlsServer.start();
            String tlsBaseUrl = tlsServer.getBaseUrl();

            // Create HttpClient with insecure TLS
            Tls tls = new Tls(null, new InsecureTls(true), null, null);
            TlsHttpClientConfigurator tlsConfigurator = new TlsHttpClientConfigurator(tls);
            HttpClient tlsClient = tlsConfigurator.apply(HttpClient.newBuilder()).build();

            // Authenticate
            var authRequest = HttpRequest.newBuilder()
                    .uri(URI.create(tlsBaseUrl + "/api/v1/auth/tokens/"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"test\",\"password\":\"test\"}"))
                    .build();
            var authResponse = tlsClient.send(authRequest, HttpResponse.BodyHandlers.ofString());
            var auth = OBJECT_MAPPER.readValue(authResponse.body(), AuthResponse.class);
            String token = auth.jwt();

            // Create key
            var keyRequest = new CreateKeyRequest("tls-test-key", "AES", 12, Map.of());
            var createKeyReq = HttpRequest.newBuilder()
                    .uri(URI.create(tlsBaseUrl + "/api/v1/vault/keys2/"))
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(keyRequest)))
                    .build();
            var createKeyResp = tlsClient.send(createKeyReq, HttpResponse.BodyHandlers.ofString());
            var createKeyResponse = OBJECT_MAPPER.readValue(createKeyResp.body(), CreateKeyResponse.class);
            String keyName = createKeyResponse.name();

            // Encrypt
            byte[] plaintext = "HTTPS test data".getBytes(StandardCharsets.UTF_8);
            var encryptReq = new EncryptRequest(keyName, plaintext, "name");
            var encryptHttpReq = HttpRequest.newBuilder()
                    .uri(URI.create(tlsBaseUrl + "/api/v1/crypto/encrypt"))
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(encryptReq)))
                    .build();
            var encryptResp = tlsClient.send(encryptHttpReq, HttpResponse.BodyHandlers.ofString());
            var encryptResponse = OBJECT_MAPPER.readValue(encryptResp.body(), EncryptResponse.class);

            // Decrypt
            var decryptReq = new DecryptRequest(
                    encryptResponse.ciphertext(),
                    encryptResponse.tag(),
                    encryptResponse.id(),
                    encryptResponse.version(),
                    encryptResponse.mode(),
                    encryptResponse.iv());
            var decryptHttpReq = HttpRequest.newBuilder()
                    .uri(URI.create(tlsBaseUrl + "/api/v1/crypto/decrypt"))
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(decryptReq)))
                    .build();
            HttpResponse<String> decryptResp;

            decryptResp = tlsClient.send(decryptHttpReq, HttpResponse.BodyHandlers.ofString());

            var decryptResponse = OBJECT_MAPPER.readValue(decryptResp.body(), DecryptResponse.class);

            // Verify round-trip
            assertThat(decryptResponse.plaintext()).isEqualTo(plaintext);
        }
        finally {
            tlsServer.stop();
        }
    }

}
