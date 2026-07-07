/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.auth;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;

import io.kroxylicious.kms.provider.thales.ciphertrust.model.AuthRequest;

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
public class ClientCertificateTokenService extends AbstractTokenService {

    private final String clientId;

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
        super(endpointUrl, timeout, tlsConfigurator);

        Objects.requireNonNull(clientId, "clientId cannot be null");
        if (clientId.isBlank()) {
            throw new IllegalArgumentException("clientId cannot be blank");
        }

        this.clientId = clientId;
    }

    @Override
    public CompletionStage<BearerToken> getBearerToken() {
        return authenticateWithCertificate();
    }

    private CompletionStage<BearerToken> authenticateWithCertificate() {
        AuthRequest request = AuthRequest.withClientCredential(clientId);
        return authenticate(request, "client certificate authentication");
    }
}
