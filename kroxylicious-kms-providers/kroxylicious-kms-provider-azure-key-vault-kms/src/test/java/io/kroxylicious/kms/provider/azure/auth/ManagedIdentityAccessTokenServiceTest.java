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
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.assertj.core.api.ThrowableAssertAlternative;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;

import io.kroxylicious.kms.provider.azure.MalformedResponseBodyException;
import io.kroxylicious.kms.provider.azure.UnexpectedHttpStatusCodeException;
import io.kroxylicious.kms.provider.azure.config.auth.ManagedIdentityConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class ManagedIdentityAccessTokenServiceTest {

    public static final String TARGET_RESOURCE = "https://example.com/";

    // adapted from error response descriptions given at https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#error-handling
    private static final String KNOWN_BAD_REQUEST_RESPONSE = """
            {
              "error": "invalid_resource",
              "error_description": "AADSTS50001: The application named <URI> wasn't found in the tenant named <TENANT-ID>. This message shows if the tenant administrator hasn't installed the application or no tenant user consented to it. You might have sent your authentication request to the wrong tenant.",
            }
            """;

    // copied directly from https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http
    private static final String KNOWN_GOOD_RESPONSE = """
            {
              "access_token": "eyJ0eXAi...",
              "refresh_token": "",
              "expires_in": "3599",
              "expires_on": "1506484173",
              "not_before": "1506480273",
              "resource": "https://management.azure.com/",
              "token_type": "Bearer"
            }
            """;

    private static WireMockServer server;

    @BeforeAll
    static void initMockServer() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
    }

    @BeforeEach
    void beforeEach() {
        server.resetAll();
    }

    @AfterAll
    static void shutdownMockServer() {
        server.shutdown();
    }

    @Test
    void getToken() {
        Instant fixedInstant = Instant.now();
        Clock clock = Clock.fixed(fixedInstant, ZoneId.of("UTC"));
        server.stubFor(tokenRespondsWithJson(KNOWN_GOOD_RESPONSE));
        try (ManagedIdentityAccessTokenService service = new ManagedIdentityAccessTokenService(
                new ManagedIdentityConfig(TARGET_RESOURCE, URI.create(server.baseUrl()).getHost(), server.port()), clock)) {
            assertThat(service.getBearerToken().toCompletableFuture()).succeedsWithin(Duration.of(5, SECONDS))
                    .satisfies(token -> {
                        assertThat(token).isNotNull();
                        assertThat(token.token()).isEqualTo("eyJ0eXAi...");
                        assertThat(token.created()).isEqualTo(fixedInstant);
                        assertThat(token.expires()).isEqualTo(fixedInstant.plusSeconds(3599));
                    });
            assertThat(server.getAllServeEvents())
                    .singleElement()
                    .extracting(ServeEvent::getRequest)
                    .satisfies(request -> {
                        assertThat(request.getMethod()).isEqualTo(RequestMethod.GET);
                        assertThat(request.getUrl()).isEqualTo(
                                "/metadata/identity/oauth2/token?api-version=2018-02-01&resource=" + URLEncoder.encode(TARGET_RESOURCE, StandardCharsets.UTF_8));
                        assertThat(request.getHeader("Metadata")).isEqualTo("true");
                    });
        }
    }

    static Stream<Arguments> invalidResponse() {
        return Stream.of(
                Arguments.argumentSet(
                        "unknown server error",
                        tokenMapping().willReturn(WireMock.aResponse().withStatus(500)),
                        (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOfSatisfying(
                                UnexpectedHttpStatusCodeException.class,
                                e -> assertThat(e.getStatusCode()).isEqualTo(500))),
                Arguments.argumentSet(
                        "redirect",
                        tokenMapping().willReturn(WireMock.aResponse().withStatus(302).withHeader("Location", "http://malicious-endpoint")),
                        (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOfSatisfying(
                                UnexpectedHttpStatusCodeException.class,
                                e -> assertThat(e.getStatusCode()).isEqualTo(302))),
                Arguments.argumentSet(
                        "bad request",
                        tokenMapping().willReturn(WireMock.aResponse().withStatus(400).withBody(KNOWN_BAD_REQUEST_RESPONSE)),
                        (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOfSatisfying(
                                UnexpectedHttpStatusCodeException.class,
                                e -> assertThat(e.getStatusCode()).isEqualTo(400))),
                Arguments.argumentSet(
                        "empty body",
                        tokenRespondsWithJson(""),
                        (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOf(MalformedResponseBodyException.class)
                                .withMessage("response body is null or empty")),
                Arguments.argumentSet(
                        "malformed json",
                        tokenRespondsWithJson("this isn't json"),
                        (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOf(MalformedResponseBodyException.class)
                                .withMessage("failed to decode response body")));
    }

    private static MappingBuilder tokenRespondsWithJson(String body) {
        return tokenMapping()
                .willReturn(WireMock.aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(body));
    }

    @NonNull
    private static MappingBuilder tokenMapping() {
        return WireMock.get("/metadata/identity/oauth2/token?api-version=2018-02-01&resource=" + URLEncoder.encode(TARGET_RESOURCE, StandardCharsets.UTF_8));
    }

    @MethodSource
    @ParameterizedTest
    void invalidResponse(MappingBuilder resp, Consumer<ThrowableAssertAlternative<?>> asserter) {
        Instant fixedInstant = Instant.now();
        Clock clock = Clock.fixed(fixedInstant, ZoneId.of("UTC"));
        server.stubFor(resp);
        try (ManagedIdentityAccessTokenService service = new ManagedIdentityAccessTokenService(
                new ManagedIdentityConfig(TARGET_RESOURCE, URI.create(server.baseUrl()).getHost(), server.port()), clock)) {
            ThrowableAssertAlternative<?> causeAssert = assertThat(service.getBearerToken().toCompletableFuture()).failsWithin(
                    Duration.of(5, SECONDS)).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause();
            asserter.accept(causeAssert);
        }
    }
}
