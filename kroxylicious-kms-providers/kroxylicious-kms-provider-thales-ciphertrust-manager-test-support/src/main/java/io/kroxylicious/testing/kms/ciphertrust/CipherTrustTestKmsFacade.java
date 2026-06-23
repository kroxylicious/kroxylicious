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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.thales.ciphertrust.CipherTrustEdek;
import io.kroxylicious.kms.provider.thales.ciphertrust.CipherTrustKmsService;
import io.kroxylicious.kms.provider.thales.ciphertrust.WrappingKey;
import io.kroxylicious.kms.provider.thales.ciphertrust.config.ClientCredentials;
import io.kroxylicious.kms.provider.thales.ciphertrust.config.Config;
import io.kroxylicious.kms.provider.thales.ciphertrust.config.UserCredentials;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthResponse;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.GetKeyResponse;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustProvider;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.testing.kms.TestKekManager;
import io.kroxylicious.testing.kms.TestKmsFacade;
import io.kroxylicious.testing.kms.ciphertrust.model.CreateKeyRequest;
import io.kroxylicious.testing.kms.ciphertrust.model.GetKeysResponse;
import io.kroxylicious.testing.kms.ciphertrust.model.RotateKeyRequest;
import io.kroxylicious.testing.kms.tls.TlsHttpClientConfigurator;

import edu.umd.cs.findbugs.annotations.Nullable;

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

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
 *
 * <h2>Environment Variables</h2>
 * <h3>Server Selection</h3>
 * <ul>
 * <li><b>KROXYLICIOUS_KMS_THALES_CIPHERTRUST_API_ENDPOINT</b> - URL of real CipherTrust Manager instance (if not set, uses mock server)</li>
 * </ul>
 *
 * <h3>Authentication Mode</h3>
 * <ul>
 * <li><b>KROXYLICIOUS_KMS_THALES_CIPHERTRUST_AUTH_MODE</b> - Authentication mode: PASSWORD or CLIENT_CERT (default: CLIENT_CERT)</li>
 * </ul>
 *
 * <h3>Password Authentication (when AUTH_MODE=PASSWORD)</h3>
 * <ul>
 * <li><b>KROXYLICIOUS_KMS_THALES_CIPHERTRUST_USERNAME</b> - Username (required for real server, ignored for mock)</li>
 * <li><b>KROXYLICIOUS_KMS_THALES_CIPHERTRUST_PASSWORD</b> - Password (required for real server, ignored for mock)</li>
 * </ul>
 *
 * <h3>Client Certificate Authentication (when AUTH_MODE=CLIENT_CERT)</h3>
 * <ul>
 * <li><b>KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_ID</b> - Client ID (required for real server, ignored for mock)</li>
 * <li><b>KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_CERT</b> - Path to client certificate PEM file (required for real server, ignored for mock)</li>
 * <li><b>KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_KEY</b> - Path to client private key PEM file (required for real server, ignored for mock)</li>
 * </ul>
 *
 * <h3>TLS Configuration (optional, for real server only)</h3>
 * <ul>
 * <li><b>KROXYLICIOUS_KMS_THALES_CIPHERTRUST_TLS_INSECURE</b> - Disable certificate verification (default: false)</li>
 * <li><b>KROXYLICIOUS_KMS_THALES_CIPHERTRUST_TLS_CA_CERT</b> - Path to custom CA certificate PEM file</li>
 * </ul>
 */
public class CipherTrustTestKmsFacade implements TestKmsFacade<Config, WrappingKey, CipherTrustEdek> {

    /**
     * Authentication mode for testing (applies to both real and mock servers).
     */
    enum AuthMode {
        /**
         * Username/password authentication.
         */
        PASSWORD,
        /**
         * Client certificate authentication.
         */
        CLIENT_CERT
    }

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
    private static final String ENV_CLIENT_ID = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_ID";
    private static final String ENV_CLIENT_CERT = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_CERT";
    private static final String ENV_CLIENT_KEY = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_KEY";
    private static final String ENV_AUTH_MODE = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_AUTH_MODE";
    private static final String ENV_TLS_INSECURE = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_TLS_INSECURE";
    private static final String ENV_TLS_CA_CERT = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_TLS_CA_CERT";
    @SuppressWarnings("java:S1075") // Ignore URIs should not be hardcoded as this path is defined by API contact
    private static final String VAULT_KEYS_PATH = "/api/v1/vault/keys2";
    @SuppressWarnings("java:S1075") // Ignore URIs should not be hardcoded as this path is defined by API contact
    private static final String AUTH_TOKENS_PATH = "/api/v1/auth/tokens/";
    private static final String ACCEPT_HEADER = "Accept";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String TEST_RUN_LABEL = "kroxylicious-test-run";
    private static final String JSON_CONTENT_TYPE = "application/json";

    private final boolean useReal;
    private final AuthMode authMode;
    private final String testRunInstance = UUID.randomUUID().toString();

    @Nullable
    private CipherTrustMockServer mockServer;

    @Nullable
    private URI cipherTrustUrl;

    @Nullable
    private ConnectionConfig connectionConfig;

    @Nullable
    private HttpClient httpClient;

    @Nullable
    private String jwtToken;

    /**
     * Creates a CipherTrust test KMS facade.
     * <p>
     * See class-level Javadoc for complete list of environment variables.
     * </p>
     */
    @SuppressWarnings("java:S2068") // Suppressed warning as this method uses a a test password
    public CipherTrustTestKmsFacade() {
        String urlStr = System.getenv(ENV_URL);
        this.useReal = urlStr != null && !urlStr.isEmpty();
        this.authMode = determineAuthMode();
        this.cipherTrustUrl = useReal ? URI.create(urlStr) : null;
        this.mockServer = null;
        this.connectionConfig = null; // Will be created in start()
    }

    private static AuthMode determineAuthMode() {
        // ENV_AUTH_MODE controls auth mode for both real and mock (defaults to CLIENT_CERT)
        return AuthMode.valueOf(System.getenv().getOrDefault(ENV_AUTH_MODE, AuthMode.CLIENT_CERT.name()).toUpperCase());
    }

    @Override
    public boolean isAvailable() {
        // Mock is always available; real is available if env var is set
        return true;
    }

    @Override
    public void start() {
        if (useReal) {
            connectionConfig = createConnectionConfigForReal();
        }
        else {
            mockServer = new CipherTrustMockServer();
            mockServer.start();
            cipherTrustUrl = URI.create(mockServer.getBaseUrl());

            boolean useClientCert = authMode == AuthMode.CLIENT_CERT;
            connectionConfig = mockServer.createConnectionConfig(useClientCert);
        }

        // Build HttpClient with TLS config (common for both real and mock)
        HttpClient.Builder clientBuilder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30));

        TlsHttpClientConfigurator tlsConfigurator = new TlsHttpClientConfigurator(connectionConfig.tls());
        tlsConfigurator.apply(clientBuilder);

        this.httpClient = clientBuilder.build();

        // Authenticate via HTTP (common for both real and mock)
        authenticateViaHttp();
    }

    private void authenticateViaHttp() {
        AuthRequest authRequest = buildAuthRequest();

        String requestBody = encodeJson(authRequest);
        HttpRequest request = buildAuthRequest(AUTH_TOKENS_PATH, requestBody);
        AuthResponse authResponse = sendRequest(request, AUTH_RESPONSE_TYPE_REF, null, 200);
        this.jwtToken = authResponse.jwt();

        LOGGER.atDebug()
                .addKeyValue("useReal", useReal)
                .addKeyValue("authMode", authMode)
                .log("Authenticated to CipherTrust Manager");
    }

    private AuthRequest buildAuthRequest() {
        Objects.requireNonNull(connectionConfig);
        return switch (authMode) {
            case PASSWORD -> {
                // Username/password authentication
                var username = Objects.requireNonNull(connectionConfig.username());
                var password = Objects.requireNonNull(connectionConfig.password());
                yield AuthRequest.withPassword(username, password);
            }
            case CLIENT_CERT -> {
                // Client certificate authentication
                var clientId = Objects.requireNonNull(connectionConfig.clientId());
                yield AuthRequest.withClientCredential(clientId);
            }
        };
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
        if (cipherTrustUrl == null || connectionConfig == null) {
            throw new IllegalStateException("CipherTrust facade not started");
        }
        return switch (authMode) {
            case PASSWORD -> {
                var userCredentials = new UserCredentials(Objects.requireNonNull(connectionConfig.username()),
                        new InlinePassword(Objects.requireNonNull(connectionConfig.password())));
                yield new Config(cipherTrustUrl, userCredentials, null, connectionConfig.tls());
            }
            case CLIENT_CERT -> {
                var clientCredentials = new ClientCredentials(Objects.requireNonNull(connectionConfig.clientId()));
                yield new Config(cipherTrustUrl, null, clientCredentials, connectionConfig.tls());
            }
        };
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new CipherTrustTestKekManager();
    }

    /**
     * Create connection configuration for real server (reads from environment variables).
     *
     * @return connection configuration
     */
    private ConnectionConfig createConnectionConfigForReal() {
        final Tls tls = createTlsForReal();
        return switch (authMode) {

            case PASSWORD -> {
                var username = System.getenv(ENV_USERNAME);
                if (username == null) {
                    throw new IllegalStateException("Environment variable " + ENV_USERNAME + " must be set for password authentication");
                }
                var password = System.getenv(ENV_PASSWORD);
                if (password == null) {
                    throw new IllegalStateException("Environment variable " + ENV_PASSWORD + " must be set for password authentication");
                }
                yield new ConnectionConfig(username, password, null, tls);
            }
            case CLIENT_CERT -> {
                var clientId = System.getenv(ENV_CLIENT_ID);
                if (clientId == null) {
                    throw new IllegalStateException("Environment variable " + ENV_CLIENT_ID + " must be set for client certificate authentication");
                }

                yield new ConnectionConfig(null, null, clientId, tls);
            }
        };
    }

    @Nullable
    private Tls createTlsForReal() {
        KeyPair keyPair = null;
        if (authMode == AuthMode.CLIENT_CERT) {
            String certPath = System.getenv(ENV_CLIENT_CERT);
            String keyPath = System.getenv(ENV_CLIENT_KEY);
            if (certPath != null && keyPath != null) {
                keyPair = new KeyPair(keyPath, certPath, null);
            }
        }

        TrustProvider trustProvider = createTrustProviderForReal();
        return keyPair != null || trustProvider != null ? new Tls(keyPair, trustProvider, null, null) : null;
    }

    @Nullable
    private TrustProvider createTrustProviderForReal() {
        // Priority 1: Insecure mode
        boolean tlsInsecure = Boolean.parseBoolean(System.getenv().getOrDefault(ENV_TLS_INSECURE, "false"));
        if (tlsInsecure) {
            return new InsecureTls(true);
        }

        // Priority 2: Custom CA cert
        String tlsCaCertPath = System.getenv(ENV_TLS_CA_CERT);
        if (tlsCaCertPath != null && !tlsCaCertPath.isEmpty()) {
            return new TrustStore(tlsCaCertPath, null, Tls.PEM, null);
        }

        // Priority 3: Platform trust store
        return null;
    }

    /**
     * Test KEK manager that makes HTTP calls to CipherTrust Manager.
     * Works identically for both mock and real instances.
     */
    private class CipherTrustTestKekManager implements TestKekManager {

        @Override
        public void generateKek(String alias) {
            CreateKeyRequest createKeyRequest = new CreateKeyRequest(
                    alias,
                    "aes",
                    12, // encrypt + decrypt
                    Map.of(TEST_RUN_LABEL, testRunInstance));

            String requestBody = encodeJson(createKeyRequest);
            HttpRequest request = buildAuthenticatedPostRequest(VAULT_KEYS_PATH + "/", requestBody);
            sendRequestNoResponse(request, 200, 201);
        }

        @Override
        public void rotateKek(String alias) {
            RotateKeyRequest rotateRequest = new RotateKeyRequest();
            String rotateBody = encodeJson(rotateRequest);
            // Use type=name parameter to explicitly pass key name instead of UUID
            HttpRequest request = buildAuthenticatedPostRequest(VAULT_KEYS_PATH + "/%s/versions/?type=name".formatted(encode(alias, UTF_8)), rotateBody);
            sendRequestNoResponse(request, 200, 201);
        }

        @Override
        public void deleteKek(String alias) {
            // Query for ALL keys with this name (handles rotated versions)
            HttpRequest queryRequest = buildAuthenticatedGetRequest(VAULT_KEYS_PATH + "?name=%s".formatted(encode(alias, UTF_8)));
            GetKeysResponse keysResponse = sendRequest(queryRequest, GET_KEYS_RESPONSE_TYPE_REF, null, 200);

            if (keysResponse.total() == 0 || keysResponse.resources() == null || keysResponse.resources().isEmpty()) {
                throw new UnknownAliasException("Key not found: " + alias);
            }

            // Delete each key version individually using type=id to be explicit
            for (GetKeyResponse key : keysResponse.resources()) {
                HttpRequest deleteRequest = buildAuthenticatedDeleteRequest(VAULT_KEYS_PATH + "/" + key.id() + "?type=id");
                sendRequestNoResponse(deleteRequest, 200, 204);
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
         * @return the matching key (with the highest version if multiple exist)
         * @throws UnknownAliasException if no key found (404)
         */
        private GetKeyResponse queryKeyByName(String alias) {
            // Use type=name parameter to explicitly query by name
            HttpRequest request = buildAuthenticatedGetRequest(VAULT_KEYS_PATH + "/%s?type=name".formatted(encode(alias, UTF_8)));
            return sendRequest(request, GET_KEY_RESPONSE_TYPE_REF, () -> new UnknownAliasException(alias), 200);
        }

        private HttpRequest buildAuthenticatedPostRequest(String path, String jsonBody) {
            return HttpRequest.newBuilder()
                    .uri(Objects.requireNonNull(cipherTrustUrl).resolve(path))
                    .header(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE)
                    .header(ACCEPT_HEADER, JSON_CONTENT_TYPE)
                    .header(AUTHORIZATION_HEADER, bearerHeader())
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();
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

    private <R> R sendRequest(HttpRequest request, TypeReference<R> valueTypeRef, @Nullable Supplier<KmsException> notFoundExceptionSupplier,
                              int... acceptableStatusCodes) {
        try {
            HttpResponse<String> response = Objects.requireNonNull(httpClient).send(request, HttpResponse.BodyHandlers.ofString());

            // Validate status code
            int statusCode = response.statusCode();
            var statusOk = isStatusOk(acceptableStatusCodes, statusCode);

            if (!statusOk) {
                if (statusCode == 404 && notFoundExceptionSupplier != null) {
                    throw notFoundExceptionSupplier.get();
                }
                throw new IllegalStateException("Request failed with status " + statusCode + ": " + response.body());
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

            var statusOk = isStatusOk(acceptableStatusCodes, response.statusCode());

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

    private boolean isStatusOk(int[] acceptableStatusCodes, int statusCode) {
        // Validate status code
        return acceptableStatusCodes.length == 0
                ? statusCode >= 200 && statusCode < 300
                : IntStream.of(acceptableStatusCodes).anyMatch(code -> code == statusCode);
    }

    private HttpRequest buildAuthRequest(String path, String jsonBody) {
        return HttpRequest.newBuilder()
                .uri(Objects.requireNonNull(cipherTrustUrl).resolve(path))
                .header(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE)
                .header(ACCEPT_HEADER, JSON_CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
    }

    private HttpRequest buildAuthenticatedGetRequest(String path) {
        return HttpRequest.newBuilder()
                .uri(Objects.requireNonNull(cipherTrustUrl).resolve(path))
                .header(ACCEPT_HEADER, JSON_CONTENT_TYPE)
                .header(AUTHORIZATION_HEADER, bearerHeader())
                .GET()
                .build();
    }

    private String bearerHeader() {
        return "Bearer " + jwtToken;
    }

    private HttpRequest buildAuthenticatedDeleteRequest(String path) {
        return HttpRequest.newBuilder()
                .uri(Objects.requireNonNull(cipherTrustUrl).resolve(path))
                .header(AUTHORIZATION_HEADER, bearerHeader())
                .DELETE()
                .build();
    }

    private void deleteTestKeks() {
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

    private List<GetKeyResponse> listKeysWithTestLabel() {
        List<GetKeyResponse> allKeys = new ArrayList<>();
        int skip = 0;
        int limit = 100;
        boolean hasMore = true;

        String labelFilter = TEST_RUN_LABEL + "=" + testRunInstance;

        while (hasMore) {
            String queryParams = "?labels=" + labelFilter + "&skip=" + skip + "&limit=" + limit;
            HttpRequest request = buildAuthenticatedGetRequest(VAULT_KEYS_PATH + queryParams);
            GetKeysResponse paginatedResponse = sendRequest(request, GET_KEYS_RESPONSE_TYPE_REF, null, 200);

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

    private void deleteKeyById(String keyId) {
        try {
            HttpRequest request = buildAuthenticatedDeleteRequest(VAULT_KEYS_PATH + "/" + keyId);
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
