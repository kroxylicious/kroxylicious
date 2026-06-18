/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust;

import java.net.URLDecoder;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformerV2;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;

import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.DecryptRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.DecryptResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.EncryptRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.EncryptResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.GetKeyResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.RandomResponse;
import io.kroxylicious.testing.certificate.CertificateGenerator;
import io.kroxylicious.testing.kms.ciphertrust.model.CreateKeyRequest;
import io.kroxylicious.testing.kms.ciphertrust.model.CreateKeyResponse;
import io.kroxylicious.testing.kms.ciphertrust.model.ErrorResponse;
import io.kroxylicious.testing.kms.ciphertrust.model.GetKeysResponse;
import io.kroxylicious.testing.kms.ciphertrust.model.RotateKeyResponse;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathTemplate;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * WireMock-based mock server simulating CipherTrust Manager REST API.
 * <p>
 * Implements real AES-GCM encryption/decryption for realistic testing.
 * Stores KEKs in-memory and validates JWT tokens.
 * </p>
 */
public class CipherTrustMockServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(CipherTrustMockServer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String MOCK_JWT_TOKEN = "mock-jwt-token";
    private static final String MOCK_REFRESH_TOKEN = "mock-refresh-token";
    private static final int TOKEN_DURATION = 300; // 5 minutes
    private static final int GCM_TAG_LENGTH_BITS = 128;
    private static final int GCM_IV_LENGTH_BYTES = 12;
    private static final String AES_GCM_CIPHER = "AES/GCM/NoPadding";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final String TRANSFORMER_AUTH = "auth";
    private static final String TRANSFORMER_CREATE_KEY = "create-key";
    private static final String TRANSFORMER_ENCRYPT = "encrypt";
    private static final String TRANSFORMER_DECRYPT = "decrypt";
    private static final String TRANSFORMER_DELETE_KEY = "delete-key";
    private static final String TRANSFORMER_GET_KEY_BY_NAME = "get-key-by-name";
    private static final String TRANSFORMER_QUERY_KEY = "query-key";
    private static final String TRANSFORMER_RANDOM_BYTES = "random-bytes";
    private static final String TRANSFORMER_ROTATE_KEY = "rotate-key";

    /**
     * Test username for mock authentication.
     */
    public static final String TEST_USERNAME = "testuser";

    /**
     * Test password for mock authentication.
     */
    @SuppressWarnings("java:S2068") // Suppressed warning as this is a test password
    public static final String TEST_PASSWORD = "testpass";

    @SuppressWarnings("java:S2068") // Suppressed warning as this is a test password
    private static final String STORE_PASSWORD = "changeit";
    @SuppressWarnings("java:S2068") // Suppressed warning as this is a test password
    private static final String KEY_PASSWORD = "keypass";
    private final WireMockServer server;
    private final boolean useTls;

    @Nullable
    private X509Certificate serverCertificate;

    /**
     * Create a CipherTrust mock server using HTTP.
     */
    public CipherTrustMockServer() {
        this(false);
    }

    /**
     * Create a CipherTrust mock server with optional TLS support.
     *
     * @param useTls if true, configure HTTPS with self-signed certificate; if false, use HTTP
     */
    public CipherTrustMockServer(boolean useTls) {
        this.useTls = useTls;
        SecureRandom secureRandom;
        try {
            secureRandom = SecureRandom.getInstanceStrong();
        }
        catch (Exception e) {
            throw new MockServerException("Failed to initialize SecureRandom", e);
        }

        VersionedKeyStore keyStore = new VersionedKeyStore();
        WireMockConfiguration config = WireMockConfiguration.options()
                .extensions(
                        new AuthTransformer(),
                        new RandomBytesTransformer(secureRandom),
                        new EncryptTransformer(keyStore, secureRandom),
                        new DecryptTransformer(keyStore),
                        new CreateKeyTransformer(keyStore),
                        new QueryKeyTransformer(keyStore),
                        new GetKeyByNameTransformer(keyStore),
                        new RotateKeyTransformer(keyStore),
                        new DeleteKeyTransformer(keyStore));

        if (useTls) {
            config.dynamicHttpsPort();
            configureTls(config);
        }
        else {
            config.dynamicPort();
        }

        this.server = new WireMockServer(config);
    }

    private void configureTls(WireMockConfiguration config) {
        try {
            KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
            X509Certificate certificate = CertificateGenerator.generateSelfSignedX509Certificate(keyPair);
            this.serverCertificate = certificate; // Store for later access
            CertificateGenerator.KeyStore keyStore = CertificateGenerator.createJksKeystore(
                    keyPair,
                    certificate,
                    STORE_PASSWORD,
                    KEY_PASSWORD);

            config.keystorePath(keyStore.path().toString())
                    .keystorePassword(STORE_PASSWORD)
                    .keyManagerPassword(KEY_PASSWORD);
        }
        catch (Exception e) {
            throw new MockServerException("Failed to configure TLS", e);
        }
    }

    /**
     * Start the mock server and configure endpoints.
     */
    public void start() {
        server.start();
        setupEndpoints();
    }

    /**
     * Stop the mock server.
     */
    public void stop() {
        if (server != null && server.isRunning()) {
            server.stop();
        }
    }

    /**
     * Get the base URL of the mock server.
     *
     * @return base URL (http or https depending on TLS configuration)
     */
    public String getBaseUrl() {
        String scheme = useTls ? "https" : "http";
        int port = useTls ? server.httpsPort() : server.port();
        return scheme + "://localhost:" + port;
    }

    /**
     * Check if the mock server is using TLS.
     *
     * @return true if using HTTPS, false if using HTTP
     */
    public boolean isHttps() {
        return useTls;
    }

    /**
     * Get the server certificate used by the mock server.
     *
     * @return the X509 certificate
     * @throws IllegalStateException if TLS is not enabled
     */
    @Nullable
    public X509Certificate getServerCertificate() {
        if (!useTls) {
            throw new IllegalStateException("Mock server not configured with TLS");
        }
        return serverCertificate;
    }

    /**
     * Get the server certificate as a PEM file.
     *
     * @return path to temporary PEM file containing the server certificate
     * @throws IllegalStateException if TLS is not enabled
     */
    public Path getServerCertificatePem() {
        return CertificateGenerator.generateCertPem(getServerCertificate());
    }

    private void setupEndpoints() {
        setupAuthEndpoint();
        setupRandomEndpoint();
        setupEncryptEndpoint();
        setupDecryptEndpoint();
        setupKeyManagementEndpoints();

        // Catch-all for debugging - log unmatched requests
        server.stubFor(WireMock.any(anyUrl())
                .atPriority(10) // Low priority so specific stubs match first
                .willReturn(aResponse()
                        .withStatus(404)
                        .withBody("No matching stub found")));
    }

    private void setupAuthEndpoint() {
        server.stubFor(post(urlPathEqualTo("/api/v1/auth/tokens/"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withTransformers(TRANSFORMER_AUTH))); // Transformer will validate credentials and set body
    }

    private void setupRandomEndpoint() {
        server.stubFor(get(urlPathMatching("/api/v1/vault/random.*"))
                .withHeader(AUTHORIZATION_HEADER, equalTo(getBearerToken()))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withTransformers(TRANSFORMER_RANDOM_BYTES))); // Transformer will set body and headers
    }

    private String getBearerToken() {
        return "Bearer " + MOCK_JWT_TOKEN;
    }

    private void setupEncryptEndpoint() {
        server.stubFor(post(urlPathEqualTo("/api/v1/crypto/encrypt"))
                .withHeader(AUTHORIZATION_HEADER, equalTo(getBearerToken()))
                .withHeader(CONTENT_TYPE_HEADER, equalTo(JSON_CONTENT_TYPE))
                .withRequestBody(matchingJsonPath("$.id"))
                .withRequestBody(matchingJsonPath("$.plaintext"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withTransformers(TRANSFORMER_ENCRYPT))); // Transformer will set body and headers
    }

    private void setupDecryptEndpoint() {
        server.stubFor(post(urlPathEqualTo("/api/v1/crypto/decrypt"))
                .withHeader(AUTHORIZATION_HEADER, equalTo(getBearerToken()))
                .withHeader(CONTENT_TYPE_HEADER, equalTo(JSON_CONTENT_TYPE))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withTransformers(TRANSFORMER_DECRYPT))); // Transformer will set body and headers
    }

    private void setupKeyManagementEndpoints() {
        // Rotate key by name (create new version) - high priority, specific pattern
        server.stubFor(post(urlPathTemplate("/api/v1/vault/keys2/{keyName}/versions/"))
                .withQueryParam("type", equalTo("name"))
                .atPriority(1)
                .withHeader(AUTHORIZATION_HEADER, equalTo(getBearerToken()))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withTransformers(TRANSFORMER_ROTATE_KEY))); // Transformer will set body and headers

        // Create key - high priority, exact path
        server.stubFor(post(urlPathEqualTo("/api/v1/vault/keys2/"))
                .atPriority(1)
                .withHeader(AUTHORIZATION_HEADER, equalTo(getBearerToken()))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withTransformers(TRANSFORMER_CREATE_KEY))); // Transformer will set body and headers

        // Delete key by ID with type=id parameter - high priority, specific HTTP method
        server.stubFor(delete(urlPathTemplate("/api/v1/vault/keys2/{keyId}"))
                .withQueryParam("type", equalTo("id"))
                .atPriority(1)
                .withHeader(AUTHORIZATION_HEADER, equalTo(getBearerToken()))
                .willReturn(aResponse()
                        .withStatus(204)
                        .withTransformers(TRANSFORMER_DELETE_KEY))); // Transformer will set body and headers

        // Get key by name with type=name parameter - higher priority than general query
        server.stubFor(get(urlPathTemplate("/api/v1/vault/keys2/{keyName}"))
                .withQueryParam("type", equalTo("name"))
                .atPriority(2)
                .withHeader(AUTHORIZATION_HEADER, equalTo(getBearerToken()))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withTransformers(TRANSFORMER_GET_KEY_BY_NAME))); // Transformer will set body and headers

        // Query keys (by name or labels) - lower priority, broader pattern
        server.stubFor(get(urlPathMatching("/api/v1/vault/keys2.*"))
                .atPriority(5)
                .withHeader(AUTHORIZATION_HEADER, equalTo(getBearerToken()))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withTransformers(TRANSFORMER_QUERY_KEY))); // Transformer will set body and headers
    }

    // Transformer implementations

    /**
     * Base transformer that ensures all exceptions are caught and returned as JSON error responses.
     */
    private abstract static class JsonErrorHandlingTransformer implements ResponseDefinitionTransformerV2 {

        @Override
        public boolean applyGlobally() {
            return false;
        }

        @Override
        public final ResponseDefinition transform(ServeEvent serveEvent) {
            try {
                return doTransform(serveEvent);
            }
            catch (Exception e) {
                LOGGER.atError()
                        .setCause(e)
                        .addKeyValue("transformer", getName())
                        .log("Transformer failed");

                ErrorResponse errorResponse = new ErrorResponse(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                return jsonResponse(errorResponse, 500);
            }
        }

        /**
         * Perform the actual transformation. Subclasses should return JSON error responses
         * for expected errors (e.g., 400, 401, 404) and throw exceptions for unexpected errors.
         */
        abstract ResponseDefinition doTransform(ServeEvent serveEvent);
    }

    /**
     * Base class for transformers that need access to the key store.
     */
    private abstract static class KeyStoreTransformer extends JsonErrorHandlingTransformer {
        final VersionedKeyStore keyStore;

        KeyStoreTransformer(VersionedKeyStore keyStore) {
            this.keyStore = keyStore;
        }
    }

    private static class AuthTransformer extends JsonErrorHandlingTransformer {

        @Override
        @SuppressFBWarnings("HARD_CODE_PASSWORD") // Test password comparison
        @SuppressWarnings("java:S2068") // allow hardcoded password as this is a test server
        ResponseDefinition doTransform(ServeEvent serveEvent) {
            AuthRequest request = parseJsonRequest(serveEvent.getRequest(), AuthRequest.class);

            String username = request.username();
            String password = request.password();
            String grantType = request.grantType();

            // Default to password grant type if not specified
            if (grantType == null) {
                grantType = "password";
            }

            LOGGER.atDebug()
                    .addKeyValue("username", username)
                    .addKeyValue("grantType", grantType)
                    .log("Processing auth request");

            // Validate credentials for password grant type
            if ("password".equals(grantType)) {
                if (!TEST_USERNAME.equals(username) || !TEST_PASSWORD.equals(password)) {
                    LOGGER.atDebug()
                            .addKeyValue("username", username)
                            .log("Authentication failed: invalid credentials");

                    ErrorResponse errorResponse = new ErrorResponse("Invalid credentials");
                    return jsonResponse(errorResponse, 401);
                }
            }
            else {
                LOGGER.atWarn()
                        .addKeyValue("grantType", grantType)
                        .log("Unsupported grant type");

                ErrorResponse errorResponse = new ErrorResponse("Unsupported grant type: " + grantType);
                return jsonResponse(errorResponse, 400);
            }

            // Successful authentication
            AuthResponse response = new AuthResponse(MOCK_JWT_TOKEN, TOKEN_DURATION, MOCK_REFRESH_TOKEN);
            return jsonResponse(response, 200);
        }

        @Override
        public String getName() {
            return TRANSFORMER_AUTH;
        }
    }

    private static class RandomBytesTransformer extends JsonErrorHandlingTransformer {
        private final SecureRandom secureRandom;

        RandomBytesTransformer(SecureRandom secureRandom) {
            this.secureRandom = secureRandom;
        }

        @Override
        ResponseDefinition doTransform(ServeEvent serveEvent) {
            int bytesParam = getQueryParam(serveEvent.getRequest(), "bytes", 32, Integer::parseInt);

            LOGGER.atDebug()
                    .addKeyValue("bytes", bytesParam)
                    .log("Processing random bytes request");

            byte[] randomBytes = new byte[bytesParam];
            secureRandom.nextBytes(randomBytes);

            RandomResponse response = new RandomResponse(randomBytes);
            return jsonResponse(response, 200);
        }

        @Override
        public String getName() {
            return TRANSFORMER_RANDOM_BYTES;
        }
    }

    private static class EncryptTransformer extends KeyStoreTransformer {
        private final SecureRandom secureRandom;

        EncryptTransformer(VersionedKeyStore keyStore, SecureRandom secureRandom) {
            super(keyStore);
            this.secureRandom = secureRandom;
        }

        @Override
        ResponseDefinition doTransform(ServeEvent serveEvent) {
            EncryptRequest request = parseJsonRequest(serveEvent.getRequest(), EncryptRequest.class);
            String keyRef = request.id();
            byte[] plaintext = request.plaintext();
            String type = request.type();

            LOGGER.atDebug()
                    .addKeyValue("keyRef", keyRef)
                    .addKeyValue("type", type)
                    .addKeyValue("plaintextLength", plaintext.length)
                    .log("Processing encrypt request");

            // NOTE: This mock server requires type="name" and performs direct name-based lookup.
            // The 'id' field must contain a key name, not a key ID.
            // This eliminates ambiguity and matches the explicit type behavior of real CTM.
            if (!"name".equals(type)) {
                ErrorResponse errorResponse = new ErrorResponse(
                        "type field must be 'name' (got: %s)".formatted(type == null ? "null" : type));
                return jsonResponse(errorResponse, 400);
            }

            // Direct name lookup only
            VersionedKeyStore.KeyMetadata metadata = keyStore.getKeyMetadataByName(keyRef);
            if (metadata == null) {
                ErrorResponse errorResponse = new ErrorResponse("Key not found");
                return jsonResponse(errorResponse, 404);
            }

            SecretKey kek = metadata.secretKey();
            String keyId = metadata.id();
            int version = metadata.version();

            // Encrypt with AES-GCM
            byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
            secureRandom.nextBytes(iv);

            byte[] ciphertextWithTag = encryptAesGcm(plaintext, kek, iv);

            // Split ciphertext and tag
            int ciphertextLength = ciphertextWithTag.length - (GCM_TAG_LENGTH_BITS / 8);
            byte[] ciphertext = new byte[ciphertextLength];
            byte[] tag = new byte[GCM_TAG_LENGTH_BITS / 8];
            System.arraycopy(ciphertextWithTag, 0, ciphertext, 0, ciphertextLength);
            System.arraycopy(ciphertextWithTag, ciphertextLength, tag, 0, tag.length);

            // Return the actual key ID and version (from the highest-version key)
            EncryptResponse response = new EncryptResponse(
                    ciphertext,
                    tag,
                    keyId,
                    version,
                    "gcm",
                    iv);

            return jsonResponse(response, 200);
        }

        @Override
        public String getName() {
            return TRANSFORMER_ENCRYPT;
        }
    }

    private static class DecryptTransformer extends KeyStoreTransformer {

        DecryptTransformer(VersionedKeyStore keyStore) {
            super(keyStore);
        }

        @Override
        ResponseDefinition doTransform(ServeEvent serveEvent) {
            DecryptRequest request = parseJsonRequest(serveEvent.getRequest(), DecryptRequest.class);
            String keyId = request.id();
            byte[] ciphertext = request.ciphertext();
            byte[] tag = request.tag();
            byte[] iv = request.iv();

            LOGGER.atDebug()
                    .addKeyValue("keyId", keyId)
                    .addKeyValue("version", request.version())
                    .log("Processing decrypt request");

            // Get the KEK (version parameter is ignored in this simplified model)
            SecretKey kek = keyStore.getKey(keyId);
            if (kek == null) {
                ErrorResponse errorResponse = new ErrorResponse("Key version not found");
                return jsonResponse(errorResponse, 404);
            }

            // Combine ciphertext and tag for GCM
            byte[] ciphertextWithTag = new byte[ciphertext.length + tag.length];
            System.arraycopy(ciphertext, 0, ciphertextWithTag, 0, ciphertext.length);
            System.arraycopy(tag, 0, ciphertextWithTag, ciphertext.length, tag.length);

            byte[] plaintext = decryptAesGcm(ciphertextWithTag, kek, iv);

            DecryptResponse response = new DecryptResponse(plaintext);
            return jsonResponse(response, 200);
        }

        @Override
        public String getName() {
            return TRANSFORMER_DECRYPT;
        }
    }

    private static class CreateKeyTransformer extends KeyStoreTransformer {

        CreateKeyTransformer(VersionedKeyStore keyStore) {
            super(keyStore);
        }

        @Override
        ResponseDefinition doTransform(ServeEvent serveEvent) {
            CreateKeyRequest request = parseJsonRequest(serveEvent.getRequest(), CreateKeyRequest.class);
            String name = request.name();
            Map<String, String> labels = request.labels();
            String keyId = UUID.randomUUID().toString();

            LOGGER.atDebug()
                    .addKeyValue("name", name)
                    .addKeyValue("keyId", keyId)
                    .log("Processing create key request");

            // Create and store key at version 0 with labels
            keyStore.createKey(keyId, name, labels);

            CreateKeyResponse response = new CreateKeyResponse(keyId, name, "aes");
            return jsonResponse(response, 200);
        }

        @Override
        public String getName() {
            return TRANSFORMER_CREATE_KEY;
        }
    }

    private static class QueryKeyTransformer extends KeyStoreTransformer {

        QueryKeyTransformer(VersionedKeyStore keyStore) {
            super(keyStore);
        }

        @Override
        ResponseDefinition doTransform(ServeEvent serveEvent) {
            var request = serveEvent.getRequest();
            var name = getQueryParam(request, "name", null, Function.identity());
            var labelFilter = getQueryParam(request, "labels", null, Function.identity());
            var skip = getQueryParam(request, "skip", 0, Integer::parseInt);
            var limit = getQueryParam(request, "limit", 10, Integer::parseInt);

            LOGGER.atDebug()
                    .addKeyValue("name", name)
                    .addKeyValue("labelFilter", labelFilter)
                    .addKeyValue("skip", skip)
                    .addKeyValue("limit", limit)
                    .log("Processing query key request");

            List<GetKeyResponse> matchingKeys;
            if (name != null) {
                // Query by name
                matchingKeys = keyStore.findByName(name);
            }
            else if (labelFilter != null) {
                // Query by labels (format: key=value)
                matchingKeys = keyStore.findByLabels(labelFilter);
            }
            else {
                // Return all keys
                matchingKeys = keyStore.findAll();
            }

            // Apply pagination
            int total = matchingKeys.size();
            int endIndex = Math.min(skip + limit, total);
            List<GetKeyResponse> page = matchingKeys.subList(skip, endIndex);

            GetKeysResponse response = new GetKeysResponse(skip, limit, total, page.isEmpty() ? null : page);
            return jsonResponse(response, 200);
        }

        @Override
        public String getName() {
            return TRANSFORMER_QUERY_KEY;
        }
    }

    private static class GetKeyByNameTransformer extends KeyStoreTransformer {

        GetKeyByNameTransformer(VersionedKeyStore keyStore) {
            super(keyStore);
        }

        @Override
        ResponseDefinition doTransform(ServeEvent serveEvent) {
            String encodedName = serveEvent.getRequest().getPathParameters().get("keyName");
            String name = URLDecoder.decode(encodedName, UTF_8);

            LOGGER.atDebug()
                    .addKeyValue("name", name)
                    .log("Processing get key by name request");

            // Find the key with the highest version for this name
            GetKeyResponse keyResponse = keyStore.findKeyByNameWithHighestVersion(name);

            if (keyResponse == null) {
                ErrorResponse errorResponse = new ErrorResponse("Key not found: " + name);
                return jsonResponse(errorResponse, 404);
            }

            return jsonResponse(keyResponse, 200);
        }

        @Override
        public String getName() {
            return TRANSFORMER_GET_KEY_BY_NAME;
        }
    }

    private static class RotateKeyTransformer extends KeyStoreTransformer {

        RotateKeyTransformer(VersionedKeyStore keyStore) {
            super(keyStore);
        }

        @Override
        ResponseDefinition doTransform(ServeEvent serveEvent) {
            String encodedName = serveEvent.getRequest().getPathParameters().get("keyName");
            String name = URLDecoder.decode(encodedName, UTF_8);

            LOGGER.atDebug()
                    .addKeyValue("name", name)
                    .log("Processing rotate key request");

            String newKeyId = keyStore.rotateKeyByName(name);
            if (newKeyId == null) {
                ErrorResponse errorResponse = new ErrorResponse("Key not found");
                return jsonResponse(errorResponse, 404);
            }

            // Return the new key ID
            RotateKeyResponse response = new RotateKeyResponse(newKeyId);
            return jsonResponse(response, 200);
        }

        @Override
        public String getName() {
            return TRANSFORMER_ROTATE_KEY;
        }
    }

    private static class DeleteKeyTransformer extends KeyStoreTransformer {

        DeleteKeyTransformer(VersionedKeyStore keyStore) {
            super(keyStore);
        }

        @Override
        ResponseDefinition doTransform(ServeEvent serveEvent) {
            String keyId = serveEvent.getRequest().getPathParameters().get("keyId");

            LOGGER.atDebug()
                    .addKeyValue("keyId", keyId)
                    .log("Processing delete key request");

            keyStore.deleteKey(keyId);

            return aResponse()
                    .withStatus(204)
                    .build();
        }

        @Override
        public String getName() {
            return TRANSFORMER_DELETE_KEY;
        }
    }

    /**
     * Extract a query parameter value from a WireMock request with type conversion.
     *
     * @param request the WireMock request
     * @param paramName the query parameter name
     * @param defaultValue the default value if parameter is not present
     * @param converter function to convert the string value to the desired type
     * @param <T> the target type
     * @return the converted value or default value if parameter is not present
     */
    @Nullable
    private static <T> T getQueryParam(Request request, String paramName, @Nullable T defaultValue, Function<String, T> converter) {
        var queryParam = request.queryParameter(paramName);
        return queryParam.isPresent() ? converter.apply(queryParam.firstValue()) : defaultValue;
    }

    /**
     * Build a JSON response with the given object and status code.
     *
     * @param body the response body object to serialize
     * @param statusCode the HTTP status code
     * @return a response definition
     */
    private static ResponseDefinition jsonResponse(Object body, int statusCode) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(body);
            return aResponse()
                    .withStatus(statusCode)
                    .withHeader(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE)
                    .withBody(json)
                    .build();
        }
        catch (Exception e) {
            throw new MockServerException("Failed to serialize response body", e);
        }
    }

    /**
     * Parse the request body as JSON.
     *
     * @param request the WireMock request
     * @param clazz the target class
     * @param <T> the target type
     * @return the parsed object
     */
    private static <T> T parseJsonRequest(Request request, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(request.getBody(), clazz);
        }
        catch (Exception e) {
            throw new MockServerException("Failed to parse request body as " + clazz.getSimpleName(), e);
        }
    }

    /**
     * Encrypt plaintext using AES-GCM.
     *
     * @param plaintext the plaintext to encrypt
     * @param kek the key encryption key
     * @param iv the initialization vector
     * @return the ciphertext with authentication tag appended
     */
    private static byte[] encryptAesGcm(byte[] plaintext, SecretKey kek, byte[] iv) {
        try {
            Cipher cipher = Cipher.getInstance(AES_GCM_CIPHER);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
            cipher.init(Cipher.ENCRYPT_MODE, kek, gcmSpec);
            return cipher.doFinal(plaintext);
        }
        catch (Exception e) {
            throw new MockServerException("Failed to encrypt data", e);
        }
    }

    /**
     * Decrypt ciphertext using AES-GCM.
     *
     * @param ciphertextWithTag the ciphertext with authentication tag appended
     * @param kek the key encryption key
     * @param iv the initialization vector
     * @return the decrypted plaintext
     */
    private static byte[] decryptAesGcm(byte[] ciphertextWithTag, SecretKey kek, byte[] iv) {
        try {
            Cipher cipher = Cipher.getInstance(AES_GCM_CIPHER);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
            cipher.init(Cipher.DECRYPT_MODE, kek, gcmSpec);
            return cipher.doFinal(ciphertextWithTag);
        }
        catch (Exception e) {
            throw new MockServerException("Failed to decrypt data", e);
        }
    }

    static class MockServerException extends RuntimeException {
        MockServerException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
