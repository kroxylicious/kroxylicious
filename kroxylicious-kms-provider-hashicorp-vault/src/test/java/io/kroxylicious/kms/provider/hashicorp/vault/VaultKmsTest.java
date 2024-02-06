/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.sun.net.httpserver.HttpServer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VaultKmsTest {

    // address in TEST-NET-1, reserved for use in example specifications and other documents
    private static final URI NON_ROUTABLE = URI.create("http://192.0.2.1/");
    private static final byte ARBITRARY_BODY_BYTE = 5;

    @Test
    void testConnectionTimeout() {
        VaultKms kms = new VaultKms(NON_ROUTABLE, "token", Duration.ofMillis(500), null);
        CompletableFuture<String> alias = kms.resolveAlias("alias");
        assertThat(alias).failsWithin(Duration.ofSeconds(2))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(HttpConnectTimeoutException.class);
    }

    @Test
    void testRequestTimeoutWaitingForHeaders() {
        HttpServer httpServer = delayResponse(Duration.ofSeconds(1), Duration.ZERO);
        URI address = URI.create("http://" + httpServer.getAddress().getHostName() + ":" + httpServer.getAddress().getPort() + "/");
        VaultKms kms = new VaultKms(address, "token", Duration.ofMillis(500), null);
        CompletableFuture<String> alias = kms.resolveAlias("alias");
        assertThat(alias).failsWithin(Duration.ofSeconds(2))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(HttpTimeoutException.class);
        httpServer.stop(0);
    }

    static Stream<Arguments> legalEnginePaths() {
        var uri = URI.create("https://localhost:1234");
        return Stream.of(
                Arguments.of("no path", uri, "/v1/transit/"),
                Arguments.of("slash only", uri.resolve("/"), "/v1/transit/"),
                Arguments.of("basic", uri.resolve("v1/transit"), "/v1/transit/"),
                Arguments.of("trailing slash", uri.resolve("v1/transit/"), "/v1/transit/"),
                Arguments.of("single namespace", uri.resolve("v1/ns1/transit"), "/v1/ns1/transit/"),
                Arguments.of("many namespaces", uri.resolve("v1/ns1/ns2/transit"), "/v1/ns1/ns2/transit/"),
                Arguments.of("non standard engine name", uri.resolve("v1/mytransit"), "/v1/mytransit/"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void legalEnginePaths(String name, URI uri, String expected) {
        var kms = new VaultKms(uri, "token", Duration.ofMillis(500), null);
        assertThat(kms.getEngineUri())
                .extracting(URI::getPath)
                .isEqualTo(expected);

    }

    static Stream<Arguments> illegalEnginePaths() {
        var uri = URI.create("https://localhost:1234");
        return Stream.of(
                Arguments.of("unrecognized version", uri.resolve("v999/transit")),
                Arguments.of("missing engine", uri.resolve("v1")),
                Arguments.of("empty engine", uri.resolve("v1/")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void illegalEnginePaths(String name, URI uri) {
        assertThatThrownBy(() -> new VaultKms(uri, "token", Duration.ofMillis(500), null))
                .isInstanceOf(IllegalArgumentException.class);

    }

    @Disabled("JDK http client request timeout cancelled after headers received currently, todo add our own timeout future")
    @Test
    void testRequestTimeoutWaitingForBody() {
        HttpServer httpServer = delayResponse(Duration.ZERO, Duration.ofSeconds(1));
        URI address = URI.create("http://" + httpServer.getAddress().getHostName() + ":" + httpServer.getAddress().getPort() + "/");
        VaultKms kms = new VaultKms(address, "token", Duration.ofMillis(500), null);
        CompletableFuture<String> alias = kms.resolveAlias("alias");
        assertThat(alias).failsWithin(Duration.ofSeconds(2))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(HttpTimeoutException.class);
        httpServer.stop(0);
    }

    public static HttpServer delayResponse(Duration delayHeaders, Duration delayResponseAfterHeaders) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLocalHost(), 0), 0);
            server.createContext("/", exchange -> {
                sleep(delayHeaders);
                exchange.sendResponseHeaders(200, 1);
                sleep(delayResponseAfterHeaders);
                exchange.getResponseBody().write(ARBITRARY_BODY_BYTE);
                exchange.close();
            });
            server.setExecutor(null); // creates a default executor
            server.start();
            return server;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sleep(Duration delayHeaders) {
        try {
            Thread.sleep(delayHeaders.toMillis());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
