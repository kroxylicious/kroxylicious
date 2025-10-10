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
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.assertj.core.api.ThrowableAssertAlternative;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;

import io.kroxylicious.kms.provider.azure.MalformedResponseBodyException;
import io.kroxylicious.kms.provider.azure.UnexpectedHttpStatusCodeException;
import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

class OauthClientCredentialsTokenServiceTest {

    private static final String TENANT_ID = "my-tenant";
    private static final String CLIENT_ID = "id";
    private static final String CLIENT_SECRET = "secret";

    // copied directly from https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#error-response-1
    private static final String KNOWN_BAD_REQUEST_RESPONSE = """
            {
              "error": "invalid_scope",
              "error_description": "AADSTS70011: The provided value for the input parameter 'scope' is not valid. The scope https://foo.microsoft.com/.default is not valid.\\r\\nTrace ID: 0000aaaa-11bb-cccc-dd22-eeeeee333333\\r\\nCorrelation ID: aaaa0000-bb11-2222-33cc-444444dddddd\\r\\nTimestamp: 2016-01-09 02:02:12Z",
              "error_codes": [
                70011
              ],
              "timestamp": "YYYY-MM-DD HH:MM:SSZ",
              "trace_id": "0000aaaa-11bb-cccc-dd22-eeeeee333333",
              "correlation_id": "aaaa0000-bb11-2222-33cc-444444dddddd"
            }
            """;

    // copied directly from https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#successful-response-1
    private static final String KNOWN_GOOD_RESPONSE = """
            {
              "token_type": "Bearer",
              "expires_in": 3599,
              "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1uQ19WWmNBVGZNNXBP..."
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

    @CsvSource(value = { "https://scope/.default", "null" }, nullValues = "null")
    @ParameterizedTest
    void getToken(@Nullable URI inputScope) {
        Instant fixedInstant = Instant.now();
        Clock clock = Clock.fixed(fixedInstant, ZoneId.of("UTC"));
        server.stubFor(tokenRespondsWithJson(KNOWN_GOOD_RESPONSE));
        try (OauthClientCredentialsTokenService service = new OauthClientCredentialsTokenService(
                new EntraIdentityConfig(URI.create(server.baseUrl()), TENANT_ID, new InlinePassword(CLIENT_ID), new InlinePassword(CLIENT_SECRET), inputScope,
                        null),
                clock)) {
            assertThat(service.getBearerToken().toCompletableFuture()).succeedsWithin(Duration.of(5, ChronoUnit.SECONDS))
                    .satisfies(token -> {
                        assertThat(token).isNotNull();
                        assertThat(token.token()).isEqualTo("eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1uQ19WWmNBVGZNNXBP...");
                        assertThat(token.created()).isEqualTo(fixedInstant);
                        assertThat(token.expires()).isEqualTo(fixedInstant.plusSeconds(3599));
                    });

            assertThat(server.getAllServeEvents())
                    .singleElement()
                    .extracting(ServeEvent::getRequest)
                    .satisfies(request -> {
                        assertThat(request.getMethod()).isEqualTo(RequestMethod.POST);
                        assertThat(request.getUrl()).isEqualTo("/" + TENANT_ID + "/oauth2/v2.0/token");
                        assertThat(request.getHeader("Content-type")).isEqualTo("application/x-www-form-urlencoded");
                        String scope = URLEncoder.encode(inputScope == null ? "https://vault.azure.net/.default" : inputScope.toString(), StandardCharsets.UTF_8);
                        assertThat(request.getBodyAsString()).isEqualTo(
                                "grant_type=client_credentials&client_id=" + CLIENT_ID + "&client_secret=" + CLIENT_SECRET + "&scope=" + scope);
                    });
        }
    }

    static Stream<Arguments> invalidResponse() {
        return Stream.of(Arguments.argumentSet("unknown server error", tokenMapping().willReturn(WireMock.aResponse().withStatus(500)),
                (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOfSatisfying(UnexpectedHttpStatusCodeException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(500))),
                Arguments.argumentSet("redirect", tokenMapping().willReturn(WireMock.aResponse().withStatus(302).withHeader("Location", "https://malicious-endpoint")),
                        (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOfSatisfying(UnexpectedHttpStatusCodeException.class,
                                e -> assertThat(e.getStatusCode()).isEqualTo(302))),
                Arguments.argumentSet("bad request", tokenMapping().willReturn(WireMock.aResponse().withStatus(400).withBody(KNOWN_BAD_REQUEST_RESPONSE)),
                        (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOfSatisfying(UnexpectedHttpStatusCodeException.class,
                                e -> assertThat(e.getStatusCode()).isEqualTo(400))),
                Arguments.argumentSet("empty body", tokenRespondsWithJson(""),
                        (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOf(MalformedResponseBodyException.class)
                                .withMessage("response body is null or empty")),
                Arguments.argumentSet("malformed json", tokenRespondsWithJson("it ain't good json"),
                        (Consumer<ThrowableAssertAlternative<?>>) throwableAssert -> throwableAssert.isInstanceOf(MalformedResponseBodyException.class)
                                .withMessage("failed to decode response body")));
    }

    private static MappingBuilder tokenRespondsWithJson(String body) {
        return tokenMapping()
                .willReturn(WireMock.aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(body));
    }

    @NonNull
    private static MappingBuilder tokenMapping() {
        return WireMock.post("/" + TENANT_ID + "/oauth2/v2.0/token");
    }

    @MethodSource
    @ParameterizedTest
    void invalidResponse(MappingBuilder resp, Consumer<ThrowableAssertAlternative<?>> asserter) {
        Instant fixedInstant = Instant.now();
        Clock clock = Clock.fixed(fixedInstant, ZoneId.of("UTC"));
        server.stubFor(resp);
        try (OauthClientCredentialsTokenService service = new OauthClientCredentialsTokenService(
                new EntraIdentityConfig(URI.create(server.baseUrl()), TENANT_ID, new InlinePassword(CLIENT_ID), new InlinePassword(CLIENT_SECRET), null, null),
                clock)) {
            ThrowableAssertAlternative<?> causeAssert = assertThat(service.getBearerToken().toCompletableFuture()).failsWithin(
                    Duration.of(5, ChronoUnit.SECONDS))
                    .withThrowableThat().isInstanceOf(ExecutionException.class).havingCause();
            asserter.accept(causeAssert);
        }
    }

}
