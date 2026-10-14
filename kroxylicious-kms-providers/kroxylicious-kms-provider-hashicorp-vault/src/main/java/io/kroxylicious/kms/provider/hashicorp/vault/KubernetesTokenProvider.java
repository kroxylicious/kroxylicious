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
import java.util.Arrays;
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
 *   {vaultUrl}/v1/auth/{authPath}/login
 * }</pre>
 * If {@code vaultNamespace} is non-null and non-empty it is sent as the
 * {@code X-Vault-Namespace} header (consistent with Vault Enterprise namespace handling).
 */
public class KubernetesTokenProvider implements VaultTokenProvider {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<VaultAuthResponse> AUTH_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final String VAULT_NAMESPACE_HEADER = "X-Vault-Namespace";
    private static final long MIN_LEASE_DURATION_MS = 1000L;
    private static final long ERROR_BACKOFF_MS = 1000L;

    private final HttpClient httpClient;
    private final URI authUrl;
    @Nullable
    private final String vaultNamespace;
    private final String role;
    private final Path tokenPath;

    @Nullable
    private CompletableFuture<String> tokenFuture;
    @Nullable
    private CompletableFuture<String> refreshFuture;
    private volatile long refreshTimeMs;
    private volatile long hardExpiryTimeMs;
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
        this.authUrl = createAuthUrl(vaultUrl, authPath);
    }

    /**
     * Gets the resolved Auth URL for Kubernetes authentication.
     *
     * @return the resolved Auth URL
     */
    public URI getAuthUrl() {
        return authUrl;
    }

    private static String withTrailingSlash(String s) {
        return s.endsWith("/") ? s : s + "/";
    }

    private URI createAuthUrl(URI vaultUrl, String authPath) {
        String base = withTrailingSlash(vaultUrl.toString()) + "v1/auth/" + withTrailingSlash(authPath) + "login";
        return URI.create(base);
    }

    @Override
    public CompletionStage<String> getToken() {
        synchronized (lock) {
            long now = System.currentTimeMillis();

            // 1. If we have a current token that hasn't reached soft refresh (80% lease time), use it directly
            if (tokenFuture != null && now < refreshTimeMs) {
                return tokenFuture.minimalCompletionStage();
            }

            // 2. If token reaches soft refresh (80% lease) but hasn't reached hard expiry (100%), return existing token
            // while triggering background refresh
            if (tokenFuture != null && now < hardExpiryTimeMs) {
                if (refreshFuture == null || refreshFuture.isDone()) {
                    refreshFuture = fetchToken();
                }
                return tokenFuture.minimalCompletionStage();
            }

            // 3. Initial state or past hard expiry: block/await on the new token fetch
            if (refreshFuture == null || refreshFuture.isDone()) {
                refreshFuture = fetchToken();
            }
            tokenFuture = refreshFuture;
            return tokenFuture.minimalCompletionStage();
        }
    }

    private CompletableFuture<String> fetchToken() {
        String jwt;
        try {
            jwt = Files.readString(tokenPath, StandardCharsets.UTF_8).trim();
        }
        catch (IOException e) {
            recordFailureBackoff();
            return CompletableFuture.failedFuture(new KmsException("Failed to read Kubernetes service account token", e));
        }

        String requestBody;
        try {
            requestBody = OBJECT_MAPPER.writeValueAsString(Map.of("jwt", jwt, "role", role));
        }
        catch (JsonProcessingException e) {
            recordFailureBackoff();
            return CompletableFuture.failedFuture(new KmsException("Failed to create Kubernetes auth request body", e));
        }

        var requestBuilder = HttpRequest.newBuilder()
                .uri(authUrl)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .header("Accept", "application/json");

        if (vaultNamespace != null && !vaultNamespace.isEmpty()) {
            requestBuilder.header(VAULT_NAMESPACE_HEADER, vaultNamespace);
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
                    finally {
                        Arrays.fill(bytes, (byte) 0);
                    }
                })
                .thenApply(authResponse -> {
                    synchronized (lock) {
                        long leaseDurationMs = Math.max(MIN_LEASE_DURATION_MS, authResponse.auth().leaseDuration() * 1000L);
                        long refreshBufferMs = (long) (leaseDurationMs * 0.20);
                        long now = System.currentTimeMillis();
                        this.refreshTimeMs = now + (leaseDurationMs - refreshBufferMs);
                        this.hardExpiryTimeMs = now + leaseDurationMs;
                    }
                    return authResponse.auth().clientToken();
                })
                .whenComplete((result, ex) -> {
                    synchronized (lock) {
                        if (ex == null && result != null) {
                            // On successful refresh, promote refreshFuture to tokenFuture
                            this.tokenFuture = CompletableFuture.completedFuture(result);
                        }
                        else {
                            recordFailureBackoff();
                        }
                    }
                });
    }

    private void recordFailureBackoff() {
        synchronized (lock) {
            long now = System.currentTimeMillis();
            // Only advance the soft-refresh time to impose a retry backoff.
            // Do NOT touch hardExpiryTimeMs — the existing token (if any) remains valid until its original hard expiry.
            this.refreshTimeMs = now + ERROR_BACKOFF_MS;
            if (this.tokenFuture == null || this.tokenFuture.isCompletedExceptionally()) {
                // No valid token at all — also reset hard expiry so callers retry promptly
                this.hardExpiryTimeMs = now + ERROR_BACKOFF_MS;
                this.tokenFuture = null;
            }
        }
    }
}
