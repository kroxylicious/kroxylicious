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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthResponse;
import io.kroxylicious.kms.service.KmsException;

/**
 * Bearer token service for CipherTrust Manager user authentication.
 * <p>
 * Handles initial authentication with username/password and subsequent token
 * refresh using refresh tokens. Intended to be wrapped with
 * {@link CachingBearerTokenService} for automatic token caching and refresh.
 * </p>
 */
public class UserAuthenticationTokenService implements BearerTokenService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserAuthenticationTokenService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final URI authEndpoint;
    private final String username;
    private final String password;
    private final HttpClient client;
    private final AtomicReference<String> refreshToken = new AtomicReference<>();

    /**
     * Create a user authentication token service.
     *
     * @param endpointUrl base URL of CipherTrust Manager instance
     * @param username username for authentication
     * @param password password for authentication
     * @param timeout HTTP request timeout
     * @param tlsConfigurator TLS configuration for HTTP client
     */
    public UserAuthenticationTokenService(URI endpointUrl,
                                          String username,
                                          String password,
                                          Duration timeout,
                                          UnaryOperator<HttpClient.Builder> tlsConfigurator) {
        Objects.requireNonNull(endpointUrl, "endpointUrl cannot be null");
        Objects.requireNonNull(username, "username cannot be null");
        Objects.requireNonNull(password, "password cannot be null");
        Objects.requireNonNull(timeout, "timeout cannot be null");
        Objects.requireNonNull(tlsConfigurator, "tlsConfigurator cannot be null");

        this.authEndpoint = endpointUrl.resolve("/api/v1/auth/tokens/");
        this.username = username;
        this.password = password;
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
        String currentRefreshToken = refreshToken.get();
        if (currentRefreshToken != null) {
            // Try refresh token first (avoids sending password)
            return refreshWithToken(currentRefreshToken)
                    .exceptionallyCompose(error -> {
                        LOGGER.atDebug()
                                .addKeyValue("error", error.getMessage())
                                .log("refresh token failed, falling back to password authentication");
                        // If refresh fails, clear token and fall back to password auth
                        refreshToken.set(null);
                        return authenticateWithPassword();
                    });
        }
        else {
            // Initial authentication with username/password
            return authenticateWithPassword();
        }
    }

    private CompletionStage<BearerToken> authenticateWithPassword() {
        AuthRequest request = AuthRequest.withPassword(username, password);
        return authenticate(request, "password authentication");
    }

    private CompletionStage<BearerToken> refreshWithToken(String token) {
        AuthRequest request = AuthRequest.withRefreshToken(token);
        return authenticate(request, "token refresh");
    }

    private CompletionStage<BearerToken> authenticate(AuthRequest authRequest, String operationType) {
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
                            LOGGER.atWarn()
                                    .addKeyValue("statusCode", response.statusCode())
                                    .addKeyValue("responseBody", body)
                                    .log("{} failed", operationType);
                            throw new KmsException("%s failed with HTTP %d".formatted(operationType, response.statusCode()));
                        }
                    })
                    .thenApply(HttpResponse::body)
                    .thenApply(this::parseAuthResponse)
                    .thenApply(authResponse -> {
                        // Store refresh token for future use
                        refreshToken.set(authResponse.refreshToken());

                        // Calculate expiry time
                        Instant now = Instant.now();
                        Instant expiresAt = now.plusSeconds(authResponse.duration());

                        LOGGER.atInfo()
                                .addKeyValue("expiresAt", expiresAt)
                                .log("{} succeeded", operationType);

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
            LOGGER.atWarn()
                    .setCause(e)
                    .addKeyValue("responseBody", responseBody)
                    .log("failed to parse authentication response");
            throw new UncheckedIOException("Failed to parse authentication response", e);
        }
    }

    @Override
    public void close() {
        // HttpClient doesn't require explicit cleanup
    }
}
