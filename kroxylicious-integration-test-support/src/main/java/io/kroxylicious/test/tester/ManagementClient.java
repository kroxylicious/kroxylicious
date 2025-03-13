/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.net.http.HttpResponse.BodyHandlers.ofString;

/**
 * Client for interacting with the admin HTTP endpoint of a kroxylicious instance.
 */
public class ManagementClient implements Closeable {
    private static final String METRICS = "metrics";

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final URI uri;

    ManagementClient(@NonNull URI uri) {
        this.uri = uri;
    }

    public HttpResponse<String> getFromAdminEndpoint(String endpoint) {
        try {
            HttpRequest request = HttpRequest.newBuilder(uri.resolve(endpoint)).GET().build();
            return httpClient.send(request, ofString());
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Scrapes the metrics from the admin endpoint.
     *
     * @return list of metrics.
     */
    @NonNull
    public List<SimpleMetric> scrapeMetrics() {
        var text = getFromAdminEndpoint(METRICS).body();
        return SimpleMetric.parse(text);
    }

    @Override
    public void close() {
        // can't close httpClient until Java 21
    }
}
