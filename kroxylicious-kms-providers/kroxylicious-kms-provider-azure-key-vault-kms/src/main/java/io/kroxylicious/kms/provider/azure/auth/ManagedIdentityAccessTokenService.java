/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.auth;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.azure.MalformedResponseBodyException;
import io.kroxylicious.kms.provider.azure.UnexpectedHttpStatusCodeException;
import io.kroxylicious.kms.provider.azure.config.auth.ManagedIdentityConfig;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * BearerTokenService implementing Azure Managed Identity app-only access token acquisition.
 * <blockquote>
 *     Managed identities for Azure resources provide Azure services with an automatically managed identity
 *     in Microsoft Entra ID. You can use this identity to authenticate to any service that supports
 *     Microsoft Entra authentication, without having credentials in your code.
 * </blockquote>
 * See: <a href="https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token">
 *     How to use managed identities for Azure resources on an Azure VM to acquire an access token
 * </a>
 * <p>
 * This is a simple client that naively sends an HTTP request for a token on every call to {@link #getBearerToken()}.
 * Caching and sharing of tokens is left to other components.
 * The token endpoint is fixed and non-configurable, as it is recommended to always use the Azure Instance Metadata
 * Service (IMDS) endpoint for this. See the "Get a token using HTTP" section at the link above for details.
 * </p>
 */
public class ManagedIdentityAccessTokenService implements BearerTokenService {

    private final ManagedIdentityConfig managedIdentityConfig;
    private final Clock clock;

    private final HttpClient httpClient;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public ManagedIdentityAccessTokenService(ManagedIdentityConfig managedIdentityConfig, Clock clock) {
        Objects.requireNonNull(managedIdentityConfig, "authentication configuration is null");
        Objects.requireNonNull(clock, "clock is null");
        this.managedIdentityConfig = managedIdentityConfig;
        this.clock = clock;
        HttpClient.Builder builder = HttpClient.newBuilder();
        httpClient = builder
                .connectTimeout(Duration.of(10, SECONDS))
                .followRedirects(HttpClient.Redirect.NEVER)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    @Override
    public CompletionStage<BearerToken> getBearerToken() {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(
                        managedIdentityConfig.identityServiceURL()
                                + "/metadata/identity/oauth2/token?api-version=2018-02-01&resource="
                                + URLEncoder.encode(managedIdentityConfig.targetResource(), StandardCharsets.UTF_8)))
                .header("Metadata", "true")
                .GET()
                .build();
        Instant messageSend = clock.instant();
        CompletableFuture<HttpResponse<String>> responseCompletableFuture = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        CompletableFuture<AccessTokenResponse> future = responseCompletableFuture.thenApply(ManagedIdentityAccessTokenService::getAccessTokenResponse);
        return future.thenApply(
                accessTokenResponse -> new BearerToken(accessTokenResponse.accessToken(), messageSend, messageSend.plus(accessTokenResponse.expiresIn(), SECONDS)));
    }

    @Override
    public void close() {
        // httpclient only closable in JDK21
    }

    private static AccessTokenResponse getAccessTokenResponse(HttpResponse<String> r) {
        if (r.statusCode() != 200) {
            throw new UnexpectedHttpStatusCodeException(r);
        }
        else {
            String body = r.body();
            if (body == null || body.isEmpty()) {
                throw new MalformedResponseBodyException("response body is null or empty");
            }
            try {
                return MAPPER.readValue(body, AccessTokenResponse.class);
            }
            catch (JsonProcessingException e) {
                throw new MalformedResponseBodyException("failed to decode response body", e);
            }
        }
    }
}
