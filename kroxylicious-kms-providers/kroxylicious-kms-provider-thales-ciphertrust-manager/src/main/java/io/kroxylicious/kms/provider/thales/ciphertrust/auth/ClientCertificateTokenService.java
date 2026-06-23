/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.auth;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthResponse;
import io.kroxylicious.kms.service.KmsException;

/**
 * Bearer token service for CipherTrust Manager client certificate authentication.
 * <p>
 * Authenticates using client certificates presented during TLS handshake.
 * Unlike user authentication, client certificate authentication does NOT support
 * refresh tokens - the client must re-authenticate with the certificate each time
 * the JWT expires.
 * </p>
 * <p>
 * Intended to be wrapped with {@link CachingBearerTokenService} for automatic
 * token caching and re-authentication.
 * </p>
 */
public class ClientCertificateTokenService implements BearerTokenService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientCertificateTokenService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final URI authEndpoint;
    private final String clientId;
    private final HttpClient client;

    /**
     * Create a client certificate authentication token service.
     *
     * @param endpointUrl base URL of CipherTrust Manager instance
     * @param clientId client ID obtained during client registration
     * @param timeout HTTP request timeout
     * @param tlsConfigurator TLS configuration for HTTP client (must include client certificate)
     */
    public ClientCertificateTokenService(URI endpointUrl,
                                         String clientId,
                                         Duration timeout,
                                         UnaryOperator<HttpClient.Builder> tlsConfigurator) {
        Objects.requireNonNull(endpointUrl, "endpointUrl cannot be null");
        Objects.requireNonNull(clientId, "clientId cannot be null");
        Objects.requireNonNull(timeout, "timeout cannot be null");
        Objects.requireNonNull(tlsConfigurator, "tlsConfigurator cannot be null");

        if (clientId.isBlank()) {
            throw new IllegalArgumentException("clientId cannot be blank");
        }

        this.authEndpoint = endpointUrl.resolve("/api/v1/auth/tokens/");
        this.clientId = clientId;
        this.client = createClient(timeout, tlsConfigurator);
    }

    private HttpClient createClient(Duration timeout, UnaryOperator<HttpClient.Builder> tlsConfigurator) {
        return tlsConfigurator.apply(HttpClient.newBuilder())
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(timeout)
                .build();
    }

    @Override
    public CompletionStage<BearerToken> getBearerToken() {
        return authenticateWithCertificate();
    }

    private CompletionStage<BearerToken> authenticateWithCertificate() {
        AuthRequest request = AuthRequest.withClientCredential(clientId);
        return authenticate(request);
    }

    private CompletionStage<BearerToken> authenticate(AuthRequest authRequest) {
        try {
            String requestBody = OBJECT_MAPPER.writeValueAsString(authRequest);

            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(authEndpoint)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody, StandardCharsets.UTF_8))
                    .build();

            return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                    .thenApply(response -> {
                        if (response.statusCode() == 200) {
                            return response;
                        }
                        else {
                            String body = new String(response.body(), StandardCharsets.UTF_8);
                            var logBuilder = LOGGER.atWarn()
                                    .addKeyValue("statusCode", response.statusCode());
                            if (LOGGER.isDebugEnabled()) {
                                logBuilder = logBuilder.addKeyValue("responseBody", body);
                            }
                            logBuilder.log(LOGGER.isDebugEnabled()
                                    ? "client certificate authentication failed"
                                    : "client certificate authentication failed, increase log level to DEBUG for response body");
                            throw new KmsException("client certificate authentication failed with HTTP %d".formatted(response.statusCode()));
                        }
                    })
                    .thenApply(HttpResponse::body)
                    .thenApply(this::parseAuthResponse)
                    .thenApply(authResponse -> {
                        // Calculate expiry time
                        Instant now = Instant.now();
                        Instant expiresAt = now.plusSeconds(authResponse.duration());

                        LOGGER.atInfo()
                                .addKeyValue("expiresAt", expiresAt)
                                .log("client certificate authentication succeeded");

                        return new BearerToken(authResponse.jwt(), now, expiresAt);
                    });
        }
        catch (IOException e) {
            LOGGER.atWarn()
                    .setCause(e)
                    .log("failed to serialize authentication request");
            return CompletableFuture.failedFuture(new KmsException("Failed to serialize authentication request", e));
        }
    }

    private AuthResponse parseAuthResponse(byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, AuthResponse.class);
        }
        catch (IOException e) {
            String responseBody = new String(bytes, StandardCharsets.UTF_8);
            var logBuilder = LOGGER.atWarn()
                    .setCause(e);
            if (LOGGER.isDebugEnabled()) {
                logBuilder = logBuilder.addKeyValue("responseBody", responseBody);
            }
            logBuilder.log(LOGGER.isDebugEnabled()
                    ? "failed to parse authentication response"
                    : "failed to parse authentication response, increase log level to DEBUG for response body");
            throw new UncheckedIOException("Failed to parse authentication response", e);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
