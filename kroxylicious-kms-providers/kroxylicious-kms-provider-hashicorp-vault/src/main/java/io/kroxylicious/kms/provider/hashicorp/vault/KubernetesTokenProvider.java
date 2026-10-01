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
 */
public class KubernetesTokenProvider implements VaultTokenProvider {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<VaultAuthResponse> AUTH_RESPONSE_TYPE_REF = new TypeReference<>() {
    };

    private final HttpClient httpClient;
    private final URI authUrl;
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
     * @param vaultTransitEngineUrl   the vault transit engine url
     * @param role                    the role
     * @param serviceAccountTokenPath the path to the service account token
     * @param authPath                the auth path
     */
    public KubernetesTokenProvider(HttpClient httpClient, URI vaultTransitEngineUrl, String role, String serviceAccountTokenPath, String authPath) {
        this.httpClient = httpClient;
        this.role = role;
        this.tokenPath = Path.of(serviceAccountTokenPath);
        this.authUrl = createAuthUrl(vaultTransitEngineUrl, authPath);
    }

    private URI createAuthUrl(URI transitUrl, String authPath) {
        String urlString = transitUrl.toString();
        if (urlString.endsWith("/transit/")) {
            urlString = urlString.substring(0, urlString.length() - "transit/".length());
        }
        else if (urlString.endsWith("/transit")) {
            urlString = urlString.substring(0, urlString.length() - "transit".length());
        }
        if (!urlString.endsWith("/")) {
            urlString += "/";
        }
        return URI.create(urlString + "auth/" + authPath + "/login");
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

        HttpRequest request = HttpRequest.newBuilder()
                .uri(authUrl)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .header("Accept", "application/json")
                .build();

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
