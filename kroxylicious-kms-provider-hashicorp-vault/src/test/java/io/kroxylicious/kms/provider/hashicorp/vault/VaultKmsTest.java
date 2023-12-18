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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import static org.assertj.core.api.Assertions.assertThat;

class VaultKmsTest {

    // address in TEST-NET-1, reserved for use in example specifications and other documents
    private static final URI NON_ROUTABLE = URI.create("http://192.0.2.1/");
    private static final byte ARBITRARY_BODY_BYTE = 5;

    @Test
    void testConnectionTimeout() {
        VaultKms kms = new VaultKms(NON_ROUTABLE, "token", Duration.ofMillis(500));
        CompletableFuture<String> alias = kms.resolveAlias("alias");
        assertThat(alias).failsWithin(Duration.ofSeconds(2))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(HttpConnectTimeoutException.class);
    }

    @Test
    void testRequestTimeoutWaitingForHeaders() {
        HttpServer httpServer = delayResponse(Duration.ofSeconds(1), Duration.ZERO);
        URI address = URI.create("http://" + httpServer.getAddress().getHostName() + ":" + httpServer.getAddress().getPort() + "/");
        VaultKms kms = new VaultKms(address, "token", Duration.ofMillis(500));
        CompletableFuture<String> alias = kms.resolveAlias("alias");
        assertThat(alias).failsWithin(Duration.ofSeconds(2))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(HttpTimeoutException.class);
        httpServer.stop(0);
    }

    @Disabled("JDK http client request timeout cancelled after headers received currently, todo add our own timeout future")
    @Test
    void testRequestTimeoutWaitingForBody() {
        HttpServer httpServer = delayResponse(Duration.ZERO, Duration.ofSeconds(1));
        URI address = URI.create("http://" + httpServer.getAddress().getHostName() + ":" + httpServer.getAddress().getPort() + "/");
        VaultKms kms = new VaultKms(address, "token", Duration.ofMillis(500));
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
