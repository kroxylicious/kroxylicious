/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.auth;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.assertj.core.api.ThrowableAssertAlternative;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.httpmock.MockServer.Header;
import io.kroxylicious.kms.httpmock.MockServer.Request;
import io.kroxylicious.kms.httpmock.MockServer.Responder;
import io.kroxylicious.kms.provider.azure.MalformedResponseBodyException;
import io.kroxylicious.kms.provider.azure.UnexpectedHttpStatusCodeException;
import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kms.httpmock.MockServer.getHttpServer;
import static org.assertj.core.api.Assertions.assertThat;

class OauthClientCredentialsTokenServiceTest {

    public static final String TOKEN = "abcdef";
    public static final String TENANT_ID = "my-tenant";
    public static final String CLIENT_ID = "id";
    public static final String CLIENT_SECRET = "secret";

    @CsvSource(value = { "https://scope/.default", "null" }, nullValues = "null")
    @ParameterizedTest
    void getToken(@Nullable String inputScope) {
        Instant fixedInstant = Instant.now();
        Clock clock = Clock.fixed(fixedInstant, ZoneId.of("UTC"));
        int expirySeconds = 3200;
        Responder resp = exchange -> {
            var response = Map.of("token_type", "Bearer", "expires_in", expirySeconds, "access_token", TOKEN);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            String responseBody = new ObjectMapper().writeValueAsString(response);
            exchange.sendResponseHeaders(200, responseBody.length());
            exchange.getResponseBody().write(responseBody.getBytes(StandardCharsets.UTF_8));
        };
        try (var server = getHttpServer(resp);
                OauthClientCredentialsTokenService service = new OauthClientCredentialsTokenService(
                        new EntraIdentityConfig(server.address(), TENANT_ID, new InlinePassword(CLIENT_ID), new InlinePassword(CLIENT_SECRET), inputScope, null),
                        clock)) {
            assertThat(service.getBearerToken().toCompletableFuture()).succeedsWithin(Duration.of(5, ChronoUnit.SECONDS))
                    .satisfies(token -> {
                        assertThat(token).isNotNull();
                        assertThat(token.token()).isEqualTo(TOKEN);
                        assertThat(token.created()).isEqualTo(fixedInstant);
                        assertThat(token.expires()).isEqualTo(fixedInstant.plusSeconds(expirySeconds));
                    });
            Request request = server.singleReceivedRequest();
            assertThat(request.method()).isEqualTo("POST");
            String scope = URLEncoder.encode(inputScope == null ? "https://vault.azure.net/.default" : inputScope, StandardCharsets.UTF_8);
            assertThat(request.body()).isEqualTo(
                    "grant_type=client_credentials&client_id=" + CLIENT_ID + "&client_secret=" + CLIENT_SECRET + "&scope=" + scope);
            assertThat(request.headers()).contains(new Header("Content-type", "application/x-www-form-urlencoded"));
            assertThat(request.requestUri()).isEqualTo(URI.create("/" + TENANT_ID + "/oauth2/v2.0/token"));
        }
    }

    static Stream<Arguments> invalidResponse() {
        return Stream.of(Arguments.argumentSet("unknown server error", (Responder) exchange -> {
            exchange.sendResponseHeaders(500, 0);
            exchange.getResponseBody().close();
        }, (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOfSatisfying(UnexpectedHttpStatusCodeException.class, e -> {
            assertThat(e.getStatusCode()).isEqualTo(500);
        })),
                Arguments.argumentSet("redirect", (Responder) exchange -> {
                    exchange.getResponseHeaders().set("Location", "https://malicious/oauth/");
                    exchange.sendResponseHeaders(302, 0);
                    exchange.getResponseBody().close();
                }, (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOfSatisfying(UnexpectedHttpStatusCodeException.class, e -> {
                    assertThat(e.getStatusCode()).isEqualTo(302);
                })),
                Arguments.argumentSet("empty body", (Responder) exchange -> {
                    exchange.sendResponseHeaders(200, 0);
                    exchange.getResponseBody().close();
                }, (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOf(MalformedResponseBodyException.class)
                        .withMessage("response body is null or empty")),
                Arguments.argumentSet("malformed json", (Responder) exchange -> {
                    String responseBody = "it aint good JSON";
                    exchange.sendResponseHeaders(200, responseBody.length());
                    exchange.getResponseBody().write(responseBody.getBytes(StandardCharsets.UTF_8));
                }, (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOf(MalformedResponseBodyException.class)
                        .withMessage("failed to decode response body")));
    }

    @MethodSource
    @ParameterizedTest
    void invalidResponse(Responder resp, Consumer<ThrowableAssertAlternative<?>> asserter) {
        Instant fixedInstant = Instant.now();
        Clock clock = Clock.fixed(fixedInstant, ZoneId.of("UTC"));
        try (var server = getHttpServer(resp);
                OauthClientCredentialsTokenService service = new OauthClientCredentialsTokenService(
                        new EntraIdentityConfig(server.address(), TENANT_ID, new InlinePassword(CLIENT_ID), new InlinePassword(CLIENT_SECRET), null, null),
                        clock)) {
            ThrowableAssertAlternative<?> causeAssert = assertThat(service.getBearerToken().toCompletableFuture()).failsWithin(
                    Duration.of(5, ChronoUnit.SECONDS))
                    .withThrowableThat().isInstanceOf(ExecutionException.class).havingCause();
            asserter.accept(causeAssert);
        }
    }

}
