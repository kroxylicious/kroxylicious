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
    private final MdsRequestGate gate;

    MdsClient(HttpClient http, MdsImpersonationConfig config) {
        this(http, config, new MdsRequestGate());
    }

    MdsClient(HttpClient http, MdsImpersonationConfig config, MdsRequestGate gate) {
        this.http = http;
        this.config = config;
        this.gate = gate;
    }

    CompletionStage<MdsToken> impersonate(String principal) {
        final MdsRequestGate.Permit permit;
        try {
            permit = gate.acquire();
        }
        catch (MdsFailure e) {
            return CompletableFuture.failedStage(e);
        }
        try {
            byte[] body = MAPPER.writeValueAsBytes(Map.of("targetPrincipalType", "User", "targetPrincipalName", principal));
            HttpRequest request = HttpRequest.newBuilder(config.mdsUrl())
                    .timeout(config.requestTimeout())
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
            var response = http.sendAsync(request, ignored -> new LimitedBodySubscriber(65536));
            // These deadlines overlap: connect/request timeouts protect HTTP progress;
            // orTimeout also bounds a stalled response body and token decoding stage.
            // They do not create three sequential requestTimeout waiting periods.
            var result = response.thenApply(reply -> {
                if (reply.statusCode() != 200) {
                    throw new MdsFailure(MdsFailure.Reason.MDS_HTTP, reply.statusCode());
                }
                return MdsToken.parse(reply.body(), MAPPER, principal);
            }).orTimeout(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
            return result.handle((token, failure) -> {
                if (failure != null) {
                    response.cancel(true);
                    var safe = MdsFailure.safe(failure);
                    gate.complete(permit, safe.serviceFailure());
                    throw safe;
                }
                gate.complete(permit, false);
                return token;
            }).minimalCompletionStage();
        }
        catch (JsonProcessingException e) {
            gate.complete(permit, false);
            return CompletableFuture.failedStage(new MdsFailure(MdsFailure.Reason.REQUEST_ENCODING));
        }
        catch (RuntimeException e) {
            var safe = MdsFailure.safe(e);
            gate.complete(permit, safe.serviceFailure());
            return CompletableFuture.failedStage(safe);
        }
    }

    @Override
    public void close() {
        gate.close();
        http.shutdownNow();
    }
}
