/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.management;

import java.io.IOException;
import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * This handler is intended to act as a last resort and reject all HTTP requests not using the GET method.
 * <p>
 * It will send a <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/405">405 Method Not Allowed</a> response in response to all HTTP methods apart from <code>GET</code>.
 * A <code>GET</code> method will be sent a <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/404">404 Not Found</a> response.
 */
public class UnsupportedHttpMethodHandler implements HttpHandler {

    @Nullable
    private final HttpHandler delegate;

    public UnsupportedHttpMethodHandler() {
        this(null);
    }

    public UnsupportedHttpMethodHandler(@Nullable HttpHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            final String requestMethod = exchange.getRequestMethod();
            if ("GET".equalsIgnoreCase(requestMethod)) {
                if (delegate != null) {
                    delegate.handle(exchange);
                }
                else {
                    exchange.getRequestBody().transferTo(OutputStream.nullOutputStream());
                    exchange.sendResponseHeaders(404, -1);
                }
            }
            else {
                exchange.getRequestBody().transferTo(OutputStream.nullOutputStream());
                exchange.getResponseHeaders().add("Allow", "GET");
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }
}
