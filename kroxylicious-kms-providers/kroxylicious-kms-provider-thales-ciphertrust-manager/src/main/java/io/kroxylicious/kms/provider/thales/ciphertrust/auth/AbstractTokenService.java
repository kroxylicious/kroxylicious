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
 * Abstract base class for CipherTrust Manager authentication token services.
 * <p>
 * Provides common HTTP request/response handling infrastructure for authentication.
 * Subclasses implement specific authentication methods (client certificate, username/password).
 * </p>
 */
abstract class AbstractTokenService implements BearerTokenService {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractTokenService.class);
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected final URI authEndpoint;
    protected final HttpClient client;

    /**
     * Create an authentication token service.
     *
     * @param endpointUrl base URL of CipherTrust Manager instance
     * @param timeout HTTP request timeout
     * @param tlsConfigurator TLS configuration for HTTP client
     */
    protected AbstractTokenService(URI endpointUrl,
                                   Duration timeout,
                                   UnaryOperator<HttpClient.Builder> tlsConfigurator) {
        Objects.requireNonNull(endpointUrl, "endpointUrl cannot be null");
        Objects.requireNonNull(timeout, "timeout cannot be null");
        Objects.requireNonNull(tlsConfigurator, "tlsConfigurator cannot be null");

        this.authEndpoint = endpointUrl.resolve("/api/v1/auth/tokens/");
        this.client = createClient(timeout, tlsConfigurator);
    }

    private HttpClient createClient(Duration timeout, UnaryOperator<HttpClient.Builder> tlsConfigurator) {
        return tlsConfigurator.apply(HttpClient.newBuilder())
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(timeout)
                .build();
    }

    /**
     * Perform authentication with the given request.
     *
     * @param authRequest the authentication request
     * @param operationType description of the operation type for logging
     * @return completion stage that completes with the bearer token
     */
    protected CompletionStage<BearerToken> authenticate(AuthRequest authRequest, String operationType) {
        try {
            LOGGER.atDebug()
                    .addKeyValue("operation", operationType)
                    .addKeyValue("authRequest", authRequest)
                    .log("sending authentication request");
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
                                    .addKeyValue("operation", operationType)
                                    .addKeyValue("statusCode", response.statusCode());
                            if (LOGGER.isDebugEnabled()) {
                                logBuilder = logBuilder.addKeyValue("responseBody", body);
                            }
                            logBuilder.log(LOGGER.isDebugEnabled()
                                    ? "authentication operation failed"
                                    : "authentication operation failed, increase log level to DEBUG for response body");
                            throw new KmsException("%s failed with HTTP %d".formatted(operationType, response.statusCode()));
                        }
                    })
                    .thenApply(HttpResponse::body)
                    .thenApply(this::parseAuthResponse)
                    .thenApply(authResponse -> createBearerToken(authResponse, operationType));
        }
        catch (IOException e) {
            LOGGER.atWarn()
                    .setCause(e)
                    .log("failed to serialize authentication request");
            return CompletableFuture.failedFuture(new KmsException("Failed to serialize authentication request", e));
        }
    }

    /**
     * Parse authentication response from JSON bytes.
     *
     * @param bytes response body bytes
     * @return parsed authentication response
     * @throws UncheckedIOException if parsing fails
     */
    protected AuthResponse parseAuthResponse(byte[] bytes) {
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

    /**
     * Create a bearer token from the authentication response.
     * Subclasses can override to extract refresh tokens or other data.
     *
     * @param authResponse the authentication response
     * @param operationType description of the operation type for logging
     * @return the bearer token
     */
    protected BearerToken createBearerToken(AuthResponse authResponse, String operationType) {
        Instant now = Instant.now();
        Instant expiresAt = now.plusSeconds(authResponse.duration());

        LOGGER.atInfo()
                .addKeyValue("expiresAt", expiresAt)
                .log("{} succeeded", operationType);

        return new BearerToken(authResponse.jwt(), now, expiresAt);
    }

    @Override
    public void close() {
        client.close();
    }
}
