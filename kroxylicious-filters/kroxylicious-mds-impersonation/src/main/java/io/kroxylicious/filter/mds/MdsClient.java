/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Shared HTTPS transport; tokens are obtained independently for each Kafka connection. */
final class MdsClient implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final HttpClient http;
    private final MdsImpersonationConfig config;

    MdsClient(HttpClient http, MdsImpersonationConfig config) {
        this.http = http;
        this.config = config;
    }

    CompletionStage<MdsToken> impersonate(String principal) {
        try {
            byte[] body = MAPPER.writeValueAsBytes(Map.of("targetPrincipalType", "User", "targetPrincipalName", principal));
            HttpRequest request = HttpRequest.newBuilder(config.mdsUrl())
                    .timeout(config.requestTimeout())
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
            var response = http.sendAsync(request, ignored -> new LimitedBodySubscriber(65536));
            var result = response.thenApply(reply -> {
                if (reply.statusCode() != 200) {
                    throw new IllegalStateException("MDS impersonation was rejected with HTTP status " + reply.statusCode());
                }
                return MdsToken.parse(reply.body(), MAPPER);
            }).orTimeout(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
            return result.whenComplete((token, failure) -> {
                if (failure != null) {
                    response.cancel(true);
                }
            }).minimalCompletionStage();
        }
        catch (JsonProcessingException e) {
            return CompletableFuture.failedStage(new IllegalArgumentException("MDS request could not be encoded"));
        }
    }

    @Override
    public void close() {
        http.shutdownNow();
    }
}
