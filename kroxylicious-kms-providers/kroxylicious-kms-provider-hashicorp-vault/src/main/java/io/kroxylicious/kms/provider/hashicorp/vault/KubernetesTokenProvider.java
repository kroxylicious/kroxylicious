/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.service.KmsException;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A VaultTokenProvider that authenticates using the Kubernetes auth method.
 *
 * <p>When Kroxylicious runs as a pod in Kubernetes, the kubelet mounts a projected
 * {@code ServiceAccount} JWT at a well-known path. This provider reads that JWT, posts it
 * to the Vault Kubernetes auth login endpoint, and exchanges it for a short-lived Vault
 * client token. The token is cached until 80% of its lease duration has elapsed, at which
 * point the next call transparently triggers a fresh login.
 *
 * <p>The auth login URL is constructed as:
 * <pre>{@code
 *   {vaultUrl}/v1/[{vaultNamespace}/]auth/{authPath}/login
 * }</pre>
 * If {@code vaultNamespace} is non-null and non-empty it is also sent as the
 * {@code X-Vault-Namespace} header (consistent with Enterprise namespace handling for Transit).
 */
public class KubernetesTokenProvider implements VaultTokenProvider {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<VaultAuthResponse> AUTH_RESPONSE_TYPE_REF = new TypeReference<>() {
    };

    private final HttpClient httpClient;
    private final URI authUrl;
    @Nullable
    private final String vaultNamespace;
    private final String role;
    private final Path tokenPath;

    @Nullable
    private CompletableFuture<String> tokenFuture;
    private long expiryTimeMs;
    private final Object lock = new Object();

    /**
     * Creates a new KubernetesTokenProvider.
     *
     * @param httpClient              the http client
     * @param vaultUrl                the vault base url
     * @param vaultNamespace          the optional vault enterprise namespace
     * @param vaultRole               the vault role
     * @param serviceAccountTokenFile the path to the service account token file
     * @param authPath                the auth path
     */
    public KubernetesTokenProvider(HttpClient httpClient, URI vaultUrl, @Nullable String vaultNamespace, String vaultRole, String serviceAccountTokenFile,
                                   String authPath) {
        this.httpClient = httpClient;
        this.vaultNamespace = vaultNamespace;
        this.role = vaultRole;
        this.tokenPath = Path.of(serviceAccountTokenFile);
        this.authUrl = createAuthUrl(vaultUrl, vaultNamespace, authPath);
    }

    /**
     * Gets the resolved Auth URL for Kubernetes authentication.
     *
     * @return the resolved Auth URL
     */
    public URI getAuthUrl() {
        return authUrl;
    }

    private URI createAuthUrl(URI vaultUrl, @Nullable String vaultNamespace, String authPath) {
        String base = vaultUrl.toString();
        if (!base.endsWith("/")) {
            base += "/";
        }
        base += "v1/";
        if (vaultNamespace != null && !vaultNamespace.isEmpty()) {
            base += vaultNamespace.endsWith("/") ? vaultNamespace : vaultNamespace + "/";
        }
        base += "auth/";
        base += authPath.endsWith("/") ? authPath : authPath + "/";
        base += "login";
        return URI.create(base);
    }

    @Override
    public CompletionStage<String> getToken() {
        synchronized (lock) {
            long now = System.currentTimeMillis();
            if (tokenFuture != null && now < expiryTimeMs) {
                return tokenFuture;
            }
            if (tokenFuture == null || tokenFuture.isDone()) {
                tokenFuture = fetchToken();
            }
            return tokenFuture;
        }
    }

    private CompletableFuture<String> fetchToken() {
        String jwt;
        try {
            jwt = Files.readString(tokenPath, StandardCharsets.UTF_8).trim();
        }
        catch (IOException e) {
            return CompletableFuture.failedFuture(new KmsException("Failed to read Kubernetes service account token", e));
        }

        String requestBody;
        try {
            requestBody = OBJECT_MAPPER.writeValueAsString(Map.of("jwt", jwt, "role", role));
        }
        catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(new KmsException("Failed to create Kubernetes auth request body", e));
        }

        var requestBuilder = HttpRequest.newBuilder()
                .uri(authUrl)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .header("Accept", "application/json");

        if (vaultNamespace != null) {
            requestBuilder.header("X-Vault-Namespace", vaultNamespace);
        }

        HttpRequest request = requestBuilder.build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> {
                    if (response.statusCode() != 200) {
                        String body = new String(response.body(), StandardCharsets.UTF_8);
                        throw new KmsException("Failed to authenticate with Vault via Kubernetes auth. Status: " + response.statusCode() + " Body: " + body);
                    }
                    return response.body();
                })
                .thenApply(bytes -> {
                    try {
                        return OBJECT_MAPPER.readValue(bytes, AUTH_RESPONSE_TYPE_REF);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("Failed to decode Vault auth response as JSON", e);
                    }
                })
                .thenApply(authResponse -> {
                    synchronized (lock) {
                        long leaseDurationMs = authResponse.auth().leaseDuration() * 1000L;
                        // Refresh token when 80% of lease duration has passed (20% safety window before hard expiry)
                        long refreshBufferMs = (long) (leaseDurationMs * 0.20);
                        this.expiryTimeMs = System.currentTimeMillis() + (leaseDurationMs - refreshBufferMs);
                    }
                    return authResponse.auth().clientToken();
                });
    }
}
