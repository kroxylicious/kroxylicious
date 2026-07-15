/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.auth;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthRequest;
import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthResponse;

/**
 * Bearer token service for CipherTrust Manager user authentication.
 * <p>
 * Handles initial authentication with username/password and subsequent token
 * refresh using refresh tokens. Intended to be wrapped with
 * {@link CachingBearerTokenService} for automatic token caching and refresh.
 * </p>
 */
public class UserAuthenticationTokenService extends AbstractTokenService {

    private final String username;
    private final String password;
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
        super(endpointUrl, timeout, tlsConfigurator);

        Objects.requireNonNull(username, "username cannot be null");
        Objects.requireNonNull(password, "password cannot be null");

        this.username = username;
        this.password = password;
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
        AuthRequest request = AuthRequest.withPassword(username, password, null);
        return authenticate(request, "password authentication");
    }

    private CompletionStage<BearerToken> refreshWithToken(String token) {
        AuthRequest request = AuthRequest.withRefreshToken(token);
        return authenticate(request, "token refresh");
    }

    @Override
    protected BearerToken createBearerToken(AuthResponse authResponse, String operationType) {
        refreshToken.set(authResponse.refreshToken());

        Instant now = Instant.now();
        Instant expiresAt = now.plusSeconds(authResponse.duration());

        LOGGER.atInfo()
                .addKeyValue("expiresAt", expiresAt)
                .log("{} succeeded", operationType);

        return new BearerToken(authResponse.jwt(), now, expiresAt);
    }
}
