/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.management;

import java.io.IOException;

import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpExchange;

/**
 * This filter is intended to act as a last resort and reject all HTTP requests not using the GET method.
 * <p>
 * It will send a <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/405">405 Method Not Allowed</a> response in response to all HTTP methods apart from <code>GET</code>.
 * A <code>GET</code> method will be sent a <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/404">404 Not Found</a> response.
 */
public class UnsupportedHttpMethodFilter extends Filter {

    public static final Filter INSTANCE = new UnsupportedHttpMethodFilter();

    private UnsupportedHttpMethodFilter() {
    }

    @Override
    public void doFilter(HttpExchange exchange, Chain chain) throws IOException {
        final String requestMethod = exchange.getRequestMethod();
        if ("GET".equalsIgnoreCase(requestMethod)) {
            chain.doFilter(exchange);
        }
        else {
            try (exchange) {
                // note while the JDK docs advise exchange.getRequestBody().transferTo(OutputStream.nullOutputStream()); we explicitly don't do that!
                // As a denial-of-service protection we don't expect anything other than GET requests so there should be no input to read.
                exchange.getResponseHeaders().add("Allow", "GET");
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }

    @Override
    public String description() {
        return "";
    }
}
