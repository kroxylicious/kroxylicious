/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VaultKmsTest {

    @Test
    void resolveWithUnknownKeyReusesConnection() {
        assertReusesConnectionsOn404(vaultKms -> {
            assertThat(vaultKms.resolveAlias("alias")).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                    .withCauseInstanceOf(UnknownAliasException.class);
        });
    }

    @Test
    void generatedDekPairWithUnknownKeyReusesConnection() {
        assertReusesConnectionsOn404(vaultKms -> {
            assertThat(vaultKms.generateDekPair("alias")).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                    .withCauseInstanceOf(UnknownKeyException.class);
        });
    }

    @Test
    void decryptEdekWithUnknownKeyReusesConnection() {
        assertReusesConnectionsOn404(vaultKms -> {
            assertThat(vaultKms.decryptEdek(new VaultEdek("unknown", new byte[]{ 1 }))).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                    .withCauseInstanceOf(UnknownKeyException.class);
        });
    }

    @Test
    void testConnectionTimeout() throws NoSuchAlgorithmException {
        var uri = URI.create("http://test:8080/v1/transit");
        Duration timeout = Duration.ofMillis(500);
        VaultKms kms = new VaultKms(uri, "token", timeout, null);
        SSLContext sslContext = SSLContext.getDefault();
        HttpClient client = kms.createClient(sslContext);
        assertThat(client.connectTimeout()).hasValue(timeout);
    }

    @Test
    void testRequestTimeoutConfiguredOnRequests() {
        var uri = URI.create("http://test:8080/v1/transit");
        Duration timeout = Duration.ofMillis(500);
        VaultKms kms = new VaultKms(uri, "token", timeout, null);
        HttpRequest build = kms.createVaultRequest().uri(uri).build();
        assertThat(build.timeout()).hasValue(timeout);
    }

    static Stream<Arguments> acceptableVaultTransitEnginePaths() {
        var uri = URI.create("https://localhost:1234");
        return Stream.of(
                Arguments.of("basic", uri.resolve("v1/transit"), "/v1/transit/"),
                Arguments.of("trailing slash", uri.resolve("v1/transit/"), "/v1/transit/"),
                Arguments.of("single namespace", uri.resolve("v1/ns1/transit"), "/v1/ns1/transit/"),
                Arguments.of("many namespaces", uri.resolve("v1/ns1/ns2/transit"), "/v1/ns1/ns2/transit/"),
                Arguments.of("non standard engine name", uri.resolve("v1/mytransit"), "/v1/mytransit/"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("acceptableVaultTransitEnginePaths")
    void acceptsVaultTransitEnginePaths(String name, URI uri, String expected) {
        var kms = new VaultKms(uri, "token", Duration.ofMillis(500), null);
        assertThat(kms.getVaultTransitEngineUri())
                .extracting(URI::getPath)
                .isEqualTo(expected);

    }

    static Stream<Arguments> unacceptableVaultTransitEnginePaths() {
        var uri = URI.create("https://localhost:1234");
        return Stream.of(
                Arguments.of("no path", uri),
                Arguments.of("missing path", uri.resolve("/")),
                Arguments.of("unrecognized API version", uri.resolve("v999/transit")),
                Arguments.of("missing engine", uri.resolve("v1")),
                Arguments.of("empty engine", uri.resolve("v1/")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unacceptableVaultTransitEnginePaths")
    void detectsUnacceptableVaultTransitEnginePaths(String name, URI uri) {
        assertThatThrownBy(() -> new VaultKms(uri, "token", Duration.ZERO, null))
                .isInstanceOf(IllegalArgumentException.class);

    }

    public static HttpServer mockVaultAlways404s(RemotePortTrackingHandler handler) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/", handler);
            server.start();
            return server;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertReusesConnectionsOn404(Consumer<VaultKms> consumer) {
        RemotePortTrackingHandler handler = new RemotePortTrackingHandler();
        HttpServer httpServer = mockVaultAlways404s(handler);
        try {
            InetSocketAddress address = httpServer.getAddress();
            String vaultAddress = "http://127.0.0.1:" + address.getPort() + "/v1/transit";
            var config = new Config(URI.create(vaultAddress), new InlinePassword("token"), null);
            VaultKms service = new VaultKmsService().buildKms(config);
            for (int i = 0; i < 5; i++) {
                consumer.accept(service);
            }
            assertThat(handler.remotePorts.size()).isEqualTo(1);
        }
        finally {
            httpServer.stop(0);
        }
    }

    static class RemotePortTrackingHandler implements HttpHandler {

        final Set<Integer> remotePorts = new HashSet<>();

        @Override
        public void handle(HttpExchange t) throws IOException {
            remotePorts.add(t.getRemoteAddress().getPort());
            String response = "not-json 123";
            t.sendResponseHeaders(404, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes(StandardCharsets.UTF_8));
            os.close();
        }
    }

}
