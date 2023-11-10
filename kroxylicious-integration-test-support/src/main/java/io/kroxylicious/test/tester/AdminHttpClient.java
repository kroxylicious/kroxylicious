/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static java.net.http.HttpResponse.BodyHandlers.ofString;

/**
 * Client for interacting with the admin HTTP endpoint of a kroxylicious instance.
 */
public class AdminHttpClient {

    /**
     * An instance of the admin http client
     */
    public static AdminHttpClient INSTANCE = new AdminHttpClient();

    private AdminHttpClient() {

    }

    private final HttpClient httpClient = HttpClient.newHttpClient();

    public HttpResponse<String> getFromAdminEndpoint(String endpoint) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:9190/" + endpoint)).GET().build();
            return httpClient.send(request, ofString());
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
