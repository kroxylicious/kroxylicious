/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.thales.ciphertrust.CipherTrustEdek;
import io.kroxylicious.kms.provider.thales.ciphertrust.CipherTrustKmsService;
import io.kroxylicious.kms.provider.thales.ciphertrust.config.Config;
import io.kroxylicious.kms.provider.thales.ciphertrust.config.UserCredentials;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.GetKeyResponse;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.testing.kms.TestKekManager;
import io.kroxylicious.testing.kms.TestKmsFacade;
import io.kroxylicious.testing.kms.ciphertrust.model.CreateKeyRequest;
import io.kroxylicious.testing.kms.ciphertrust.model.GetKeysResponse;
import io.kroxylicious.testing.kms.ciphertrust.model.RotateKeyRequest;
import io.kroxylicious.testing.kms.tls.TlsHttpClientConfigurator;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Test facade for CipherTrust Manager supporting both mock and real instances.
 * <p>
 * Behavior is determined by environment variables:
 * </p>
 * <ul>
 * <li>If KROXYLICIOUS_KMS_THALES_CIPHERTRUST_API_ENDPOINT is not set: uses WireMock-based mock server with HTTP calls</li>
 * <li>If KROXYLICIOUS_KMS_THALES_CIPHERTRUST_API_ENDPOINT is set: connects to real CipherTrust instance</li>
 * </ul>
 * <p>
 * Both modes use HTTP calls for all operations including authentication,
 * ensuring identical behavior from a protocol perspective.
 * </p>
 */
public class CipherTrustTestKmsFacade implements TestKmsFacade<Config, String, CipherTrustEdek> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CipherTrustTestKmsFacade.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<AuthResponse> AUTH_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<GetKeysResponse> GET_KEYS_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<GetKeyResponse> GET_KEY_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final String ENV_URL = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_API_ENDPOINT";
    private static final String ENV_USERNAME = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_USERNAME";
    private static final String ENV_PASSWORD = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_PASSWORD";
    private static final String ENV_TLS_INSECURE = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_TLS_INSECURE";
    private static final String ENV_TLS_CA_CERT = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_TLS_CA_CERT";
    private static final String TEST_RUN_LABEL = "kroxylicious-test-run";
    private static final String TEST_USERNAME = "testuser";
    @SuppressWarnings("java:S2068") // Suppressed warning as this is a test password
    private static final String TEST_PASSWORD = "testpass";
    private static final String JSON_CONTENT_TYPE = "application/json";

    private final boolean useReal;
    private final String username;
    private final String password;
    private final boolean tlsInsecure;
    @Nullable
    private final String tlsCaCertPath;
    private final String testRunInstance = UUID.randomUUID().toString();

    @Nullable
    private CipherTrustMockServer mockServer;

    @Nullable
    private URI cipherTrustUrl;

    @Nullable
    private HttpClient httpClient;

    @Nullable
    private String jwtToken;

    /**
     * Creates a CipherTrust test KMS facade.
     */
    @SuppressWarnings("java:S2068") // Suppressed warning as this method uses a a test password
    public CipherTrustTestKmsFacade() {
        String urlStr = System.getenv(ENV_URL);
        if (urlStr != null && !urlStr.isEmpty()) {
            // Use real instance
            this.useReal = true;
            this.cipherTrustUrl = URI.create(urlStr);
            this.username = System.getenv().getOrDefault(ENV_USERNAME, TEST_USERNAME);
            this.password = System.getenv().getOrDefault(ENV_PASSWORD, TEST_PASSWORD);
            this.tlsInsecure = Boolean.parseBoolean(System.getenv().getOrDefault(ENV_TLS_INSECURE, "false"));
            this.tlsCaCertPath = System.getenv(ENV_TLS_CA_CERT);
            this.mockServer = null;
        }
        else {
            // Use mock
            this.useReal = false;
            this.cipherTrustUrl = null; // Will be set after mockServer.start()
            this.username = TEST_USERNAME;
            this.password = TEST_PASSWORD;
            this.tlsInsecure = false;
            this.tlsCaCertPath = null;
            this.mockServer = null; // Will be created in start()
        }
    }

    @Override
    public boolean isAvailable() {
        // Mock is always available; real is available if env var is set
        return true;
    }

    @Override
    public void start() {
        if (useReal) {
            startReal();
        }
        else {
            startMock();
        }
    }

    private void startMock() {
        mockServer = new CipherTrustMockServer();
        mockServer.start();
        cipherTrustUrl = URI.create(mockServer.getBaseUrl());

        // Build HttpClient with TLS config if mock uses HTTPS
        HttpClient.Builder clientBuilder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30));

        if (mockServer.isHttps()) {
            // Mock server uses self-signed certificate, so we need InsecureTls
            TlsHttpClientConfigurator tlsConfigurator = new TlsHttpClientConfigurator(
                    new Tls(null, new InsecureTls(true), null, null));
            tlsConfigurator.apply(clientBuilder);
        }

        this.httpClient = clientBuilder.build();

        // Authenticate to mock server via HTTP (same flow as real)
        authenticateViaHttp();
    }

    private void startReal() {
        // Build HttpClient with TLS config
        HttpClient.Builder clientBuilder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30));

        TlsHttpClientConfigurator tlsConfigurator = new TlsHttpClientConfigurator(getTlsConfig());
        tlsConfigurator.apply(clientBuilder);

        this.httpClient = clientBuilder.build();

        // Authenticate to real instance via HTTP
        authenticateViaHttp();
    }

    private void authenticateViaHttp() {
        try {
            AuthRequest authRequest = AuthRequest.withPassword(username, password);
            String requestBody = encodeJson(authRequest);
            HttpRequest request = buildAuthRequest("/api/v1/auth/tokens/", requestBody);
            AuthResponse authResponse = sendRequest(request, AUTH_RESPONSE_TYPE_REF, 200);
            this.jwtToken = authResponse.jwt();

            LOGGER.atDebug()
                    .addKeyValue("useReal", useReal)
                    .log("Authenticated to CipherTrust Manager");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to authenticate to CipherTrust Manager", e);
        }
    }

    @Override
    public void stop() {
        LOGGER.atInfo()
                .addKeyValue("testRunInstance", testRunInstance)
                .addKeyValue("useReal", useReal)
                .log("Stopping CipherTrust test facade and cleaning up test keys");
        try {
            deleteTestKeks();
            LOGGER.atInfo().log("Test key cleanup completed successfully");
        }
        catch (Exception e) {
            LOGGER.atWarn()
                    .setCause(e)
                    .addKeyValue("error", e.getMessage())
                    .log("Failed to clean up test keys");
        }
        finally {
            if (mockServer != null) {
                mockServer.stop();
            }
        }
    }

    @Override
    public Class<CipherTrustKmsService> getKmsServiceClass() {
        return CipherTrustKmsService.class;
    }

    @Override
    public Config getKmsServiceConfig() {
        if (cipherTrustUrl == null) {
            throw new IllegalStateException("CipherTrust facade not started");
        }
        UserCredentials userCredentials = new UserCredentials(username, new InlinePassword(password));
        return new Config(cipherTrustUrl, userCredentials, null, getTlsConfig());
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new CipherTrustTestKekManager();
    }

    @Nullable
    private Tls getTlsConfig() {
        // Priority 1: Check for insecure mode (testing/development)
        if (tlsInsecure) {
            return new Tls(null, new InsecureTls(true), null, null);
        }

        // Priority 2: For mock server, use its self-signed certificate
        if (mockServer != null && mockServer.isHttps()) {
            java.nio.file.Path caCertPem = mockServer.getServerCertificatePem();
            io.kroxylicious.proxy.config.tls.TrustStore trustStore = new io.kroxylicious.proxy.config.tls.TrustStore(
                    caCertPem.toString(),
                    null, // No password for PEM trust store
                    Tls.PEM,
                    null);
            return new Tls(null, trustStore, null, null);
        }

        // Priority 3: For real server with custom CA cert (self-signed)
        if (tlsCaCertPath != null && !tlsCaCertPath.isEmpty()) {
            io.kroxylicious.proxy.config.tls.TrustStore trustStore = new io.kroxylicious.proxy.config.tls.TrustStore(
                    tlsCaCertPath,
                    null, // No password for PEM trust store
                    Tls.PEM,
                    null);
            return new Tls(null, trustStore, null, null);
        }

        // Priority 4: Use platform trust store (works with public CA certs)
        return null;
    }

    /**
     * Test KEK manager that makes HTTP calls to CipherTrust Manager.
     * Works identically for both mock and real instances.
     */
    private class CipherTrustTestKekManager implements TestKekManager {

        @Override
        public void generateKek(String alias) {
            try {
                CreateKeyRequest createKeyRequest = new CreateKeyRequest(
                        alias,
                        "aes",
                        12, // encrypt + decrypt
                        Map.of(TEST_RUN_LABEL, testRunInstance));

                String requestBody = encodeJson(createKeyRequest);
                HttpRequest request = buildAuthenticatedPostRequest("/api/v1/vault/keys2/", requestBody);
                sendRequestNoResponse(request, 200, 201);
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to create key: " + alias, e);
            }
        }

        @Override
        public void rotateKek(String alias) {
            try {
                RotateKeyRequest rotateRequest = new RotateKeyRequest();
                String rotateBody = encodeJson(rotateRequest);
                // Use type=name parameter to explicitly pass key name instead of UUID
                HttpRequest request = buildAuthenticatedPostRequest("/api/v1/vault/keys2/" + alias + "/versions/?type=name", rotateBody);
                sendRequestNoResponse(request, 200, 201);
            }
            catch (UnknownAliasException e) {
                throw e;
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to rotate key: " + alias, e);
            }
        }

        @Override
        public void deleteKek(String alias) {
            try {
                // Query for ALL keys with this name (handles rotated versions)
                HttpRequest queryRequest = buildAuthenticatedGetRequest("/api/v1/vault/keys2?name=" + alias);
                GetKeysResponse keysResponse = sendRequest(queryRequest, GET_KEYS_RESPONSE_TYPE_REF, 200);

                if (keysResponse.total() == 0 || keysResponse.resources() == null || keysResponse.resources().isEmpty()) {
                    throw new UnknownAliasException("Key not found: " + alias);
                }

                // Delete each key version individually using type=id to be explicit
                for (GetKeyResponse key : keysResponse.resources()) {
                    HttpRequest deleteRequest = buildAuthenticatedDeleteRequest("/api/v1/vault/keys2/" + key.id() + "?type=id");
                    sendRequestNoResponse(deleteRequest, 200, 204);
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to delete key: " + alias, e);
            }
        }

        @Override
        public GetKeyResponse read(String alias) {
            return queryKeyByName(alias);
        }

        /**
         * Query a key by name using the type=name parameter.
         * CTM returns the key with the highest version for this name (handles rotation).
         *
         * @param alias key name to query
         * @return the matching key (with highest version if multiple exist)
         * @throws UnknownAliasException if no key found (404)
         */
        private GetKeyResponse queryKeyByName(String alias) {
            // Use type=name parameter to explicitly query by name
            HttpRequest request = buildAuthenticatedGetRequest("/api/v1/vault/keys2/" + alias + "?type=name");
            return sendRequest(request, GET_KEY_RESPONSE_TYPE_REF, 200);
        }
    }

    private String encodeJson(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to encode JSON", e);
        }
    }

    private static <T> T decodeJson(TypeReference<T> typeRef, String body) {
        try {
            return OBJECT_MAPPER.readValue(body, typeRef);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to decode JSON", e);
        }
    }

    private <R> R sendRequest(HttpRequest request, TypeReference<R> valueTypeRef, int... acceptableStatusCodes) {
        try {
            HttpResponse<String> response = Objects.requireNonNull(httpClient).send(request, HttpResponse.BodyHandlers.ofString());

            // Validate status code
            boolean statusOk = acceptableStatusCodes.length == 0
                    ? response.statusCode() >= 200 && response.statusCode() < 300
                    : IntStream.of(acceptableStatusCodes).anyMatch(code -> code == response.statusCode());

            if (!statusOk) {
                throw new IllegalStateException("Request failed with status " + response.statusCode() + ": " + response.body());
            }

            // Deserialize response
            return decodeJson(valueTypeRef, response.body());
        }
        catch (IOException e) {
            throw new UncheckedIOException("HTTP request failed", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Request interrupted", e);
        }
    }

    private void sendRequestNoResponse(HttpRequest request, int... acceptableStatusCodes) {
        try {
            HttpResponse<String> response = Objects.requireNonNull(httpClient).send(request, HttpResponse.BodyHandlers.ofString());

            // Validate status code
            boolean statusOk = acceptableStatusCodes.length == 0
                    ? response.statusCode() >= 200 && response.statusCode() < 300
                    : IntStream.of(acceptableStatusCodes).anyMatch(code -> code == response.statusCode());

            if (!statusOk) {
                throw new IllegalStateException("Request failed with status " + response.statusCode() + ": " + response.body());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("HTTP request failed", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Request interrupted", e);
        }
    }

    private HttpRequest buildAuthRequest(String path, String jsonBody) {
        return HttpRequest.newBuilder()
                .uri(Objects.requireNonNull(cipherTrustUrl).resolve(path))
                .header("Content-Type", JSON_CONTENT_TYPE)
                .header("Accept", JSON_CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
    }

    private HttpRequest buildAuthenticatedGetRequest(String path) {
        return HttpRequest.newBuilder()
                .uri(Objects.requireNonNull(cipherTrustUrl).resolve(path))
                .header("Accept", JSON_CONTENT_TYPE)
                .header("Authorization", bearerHeader())
                .GET()
                .build();
    }

    private String bearerHeader() {
        return "Bearer " + jwtToken;
    }

    private HttpRequest buildAuthenticatedPostRequest(String path, String jsonBody) {
        return HttpRequest.newBuilder()
                .uri(Objects.requireNonNull(cipherTrustUrl).resolve(path))
                .header("Content-Type", JSON_CONTENT_TYPE)
                .header("Accept", JSON_CONTENT_TYPE)
                .header("Authorization", bearerHeader())
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
    }

    private HttpRequest buildAuthenticatedDeleteRequest(String path) {
        return HttpRequest.newBuilder()
                .uri(Objects.requireNonNull(cipherTrustUrl).resolve(path))
                .header("Authorization", bearerHeader())
                .DELETE()
                .build();
    }

    private void deleteTestKeks() {
        try {
            List<GetKeyResponse> keys = listKeysWithTestLabel();
            LOGGER.atInfo()
                    .addKeyValue("totalKeys", keys.size())
                    .addKeyValue("testRunInstance", testRunInstance)
                    .log("Retrieved keys for cleanup");

            for (GetKeyResponse key : keys) {
                String keyId = key.id();
                String keyName = key.name();
                LOGGER.atInfo()
                        .addKeyValue("keyId", keyId)
                        .addKeyValue("keyName", keyName)
                        .log("Deleting test key");
                deleteKeyById(keyId);
            }
            LOGGER.atInfo()
                    .addKeyValue("deletedCount", keys.size())
                    .log("Deleted test keys");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to delete test keys", e);
        }
    }

    private List<GetKeyResponse> listKeysWithTestLabel() {
        try {
            List<GetKeyResponse> allKeys = new java.util.ArrayList<>();
            int skip = 0;
            int limit = 100;
            boolean hasMore = true;

            String labelFilter = TEST_RUN_LABEL + "=" + testRunInstance;

            while (hasMore) {
                String queryParams = "?labels=" + labelFilter + "&skip=" + skip + "&limit=" + limit;
                HttpRequest request = buildAuthenticatedGetRequest("/api/v1/vault/keys2" + queryParams);
                GetKeysResponse paginatedResponse = sendRequest(request, GET_KEYS_RESPONSE_TYPE_REF, 200);

                if (paginatedResponse.resources() != null && !paginatedResponse.resources().isEmpty()) {
                    allKeys.addAll(paginatedResponse.resources());
                    skip += paginatedResponse.resources().size();

                    // Check if there are more results
                    hasMore = skip < paginatedResponse.total();
                }
                else {
                    hasMore = false;
                }
            }

            return allKeys;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to list keys", e);
        }
    }

    private void deleteKeyById(String keyId) {
        try {
            HttpRequest request = buildAuthenticatedDeleteRequest("/api/v1/vault/keys2/" + keyId);
            sendRequestNoResponse(request, 200, 204);
        }
        catch (Exception e) {
            LOGGER.atWarn()
                    .setCause(e)
                    .addKeyValue("keyId", keyId)
                    .addKeyValue("error", e.getMessage())
                    .log("Failed to delete key");
        }
    }
}
