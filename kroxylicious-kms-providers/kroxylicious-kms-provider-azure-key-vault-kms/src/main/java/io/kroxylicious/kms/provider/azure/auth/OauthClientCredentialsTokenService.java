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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.azure.MalformedResponseBodyException;
import io.kroxylicious.kms.provider.azure.UnexpectedHttpStatusCodeException;
import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.proxy.tls.TlsHttpClientConfigurator;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * BearerTokenService implementing the Oauth2 Client Credentials flow implemented by Microsoft Entra
 *<blockquote>
 *  The OAuth 2.0 client credentials grant flow permits a web service (confidential client) to use its own
 *  credentials, instead of impersonating a user, to authenticate when calling another web service. The
 *  grant specified in RFC 6749, sometimes called two-legged OAuth, can be used to access web-hosted resources
 *  by using the identity of an application. This type is commonly used for server-to-server interactions
 *  that must run in the background, without immediate interaction with a user, and is often referred to
 *  as daemons or service accounts.
 * </blockquote>
 * See: <a href="https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow">
 *     Microsoft identity platform and the OAuth 2.0 client credentials flow
 * </a>
 * <p>
 * This is a simple client that naively sends an HTTP request for a token on every call to {@link #getBearerToken()}.
 * Caching and sharing of tokens is left to other components.
 * Note that the oauth endpoint and scope are configurable as there are several Azure national/sovereign clouds that
 * are isolated, with their own login endpoint and vault service domains.
 * </p>
 */
public class OauthClientCredentialsTokenService implements BearerTokenService {

    private final EntraIdentityConfig entraIdentityConfig;
    private final Clock clock;

    private final HttpClient httpClient;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public OauthClientCredentialsTokenService(EntraIdentityConfig entraIdentityConfig, Clock clock) {
        Objects.requireNonNull(entraIdentityConfig, "authConfiguration is null");
        Objects.requireNonNull(clock, "clock is null");
        this.entraIdentityConfig = entraIdentityConfig;
        this.clock = clock;
        HttpClient.Builder builder = HttpClient.newBuilder();
        var tlsConfigurator = new TlsHttpClientConfigurator(entraIdentityConfig.tls());
        tlsConfigurator.apply(builder);
        httpClient = builder
                .connectTimeout(Duration.of(10, SECONDS))
                .followRedirects(HttpClient.Redirect.NEVER)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    @Override
    public CompletionStage<BearerToken> getBearerToken() {
        HttpRequest request = getHttpRequest();
        Instant messageSend = clock.instant();
        CompletableFuture<HttpResponse<String>> responseCompletableFuture = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        CompletableFuture<AccessTokenResponse> future = responseCompletableFuture.thenApply(OauthClientCredentialsTokenService::getAccessTokenResponse);
        return future.thenApply(
                accessTokenResponse -> new BearerToken(accessTokenResponse.accessToken(), messageSend, messageSend.plus(accessTokenResponse.expiresIn(), SECONDS)));
    }

    @Override
    public void close() {
        // httpclient only closable in JDK21, use reflection?
    }

    private HttpRequest getHttpRequest() {
        Map<String, String> formData = new LinkedHashMap<>();

        formData.put("grant_type", "client_credentials");
        formData.put("client_id", entraIdentityConfig.clientId().getProvidedPassword());
        formData.put("client_secret", entraIdentityConfig.clientSecret().getProvidedPassword());
        formData.put("scope", entraIdentityConfig.getAuthScope().toString());

        String formBody = formData.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8) + "="
                        + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));

        String tokenUrl = entraIdentityConfig.getOauthEndpointOrDefault() + "/" + entraIdentityConfig.tenantId() + "/oauth2/v2.0/token";

        return HttpRequest.newBuilder()
                .uri(URI.create(tokenUrl))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(formBody))
                .build();
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
