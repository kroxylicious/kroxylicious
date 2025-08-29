/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.httpmock;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import static org.assertj.core.api.Assertions.assertThat;

public record MockServer(HttpServer server, ArrayBlockingQueue<Request> receivedRequests) implements AutoCloseable {

    public record Header(String key, String value) {

    }

    public record Request(String body, List<Header> headers, String method, URI requestUri) {

    }

    public interface Responder {
        void respond(HttpExchange exchange) throws Exception;
    }

    public static MockServer getHttpServer(Responder responder) {
        try {
            ArrayBlockingQueue<Request> requests = new ArrayBlockingQueue<>(100);
            HttpServer httpServer = HttpServer.create();
            httpServer.createContext("/", exchange -> {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                List<Header> headers = exchange.getRequestHeaders().entrySet().stream()
                        .flatMap(stringListEntry -> stringListEntry.getValue().stream().map(s -> new Header(stringListEntry.getKey(), s))).toList();
                requests.add(new Request(body, headers, exchange.getRequestMethod(), exchange.getRequestURI()));
                try {
                    responder.respond(exchange);
                }
                catch (Exception e) {
                    exchange.sendResponseHeaders(500, 0);
                }
            });
            httpServer.bind(new InetSocketAddress(Inet4Address.getLoopbackAddress(), 0), 1000);
            httpServer.start();
            return new MockServer(httpServer, requests);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }

    public String address() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    public Request singleReceivedRequest() {
        ArrayList<Request> requests = new ArrayList<>();
        this.receivedRequests.drainTo(requests);
        assertThat(requests).hasSize(1);
        return requests.get(0);
    }
}
