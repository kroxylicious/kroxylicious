/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MdsClientResilienceTest {
    private final HttpClient http = mock(HttpClient.class);
    private final AtomicLong time = new AtomicLong();
    private final ConcurrentLinkedQueue<CompletableFuture<HttpResponse<byte[]>>> exchanges = new ConcurrentLinkedQueue<>();
    private final MdsClient client;

    MdsClientResilienceTest() {
        doAnswer(ignored -> {
            var exchange = new CompletableFuture<HttpResponse<byte[]>>();
            exchanges.add(exchange);
            return exchange;
        }).when(http).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
        var config = new MdsImpersonationConfig(URI.create("https://mds/security/1.0/impersonate"),
                new Tls(new KeyPair("key", "cert", null), null, null, null), Duration.ofMinutes(1), Duration.ofSeconds(5));
        client = new MdsClient(http, config, new MdsRequestGate(time::get, () -> 0));
    }

    @Test
    void boundsConcurrentRequestsAcrossReconnects() throws Exception {
        // Given
        var requests = new ConcurrentLinkedQueue<CompletableFuture<MdsToken>>();
        try (var workers = Executors.newFixedThreadPool(16)) {
            var submissions = new ArrayList<java.util.concurrent.Future<?>>();

            // When
            for (int i = 0; i < 200; i++) {
                submissions.add(workers.submit(() -> {
                    requests.add(client.impersonate("alice").toCompletableFuture());
                }));
            }
            for (var submission : submissions) {
                submission.get(10, TimeUnit.SECONDS);
            }

            // Then
            assertThat(exchanges).hasSize(MdsRequestGate.MAX_IN_FLIGHT);
            assertThat(requests.stream().filter(CompletableFuture::isCompletedExceptionally)).hasSize(200 - MdsRequestGate.MAX_IN_FLIGHT);
        }
        finally {
            exchanges.forEach(future -> future.completeExceptionally(new java.io.IOException("unavailable")));
            client.close();
        }
    }

    @Test
    void suppressesReconnectStormAndAllowsOnlyOneRecoveryProbe() {
        // Given
        var first = client.impersonate("alice").toCompletableFuture();
        exchanges.remove().complete(reply(503));

        // When
        var refused = new ArrayList<CompletableFuture<MdsToken>>();
        for (int i = 0; i < 200; i++) {
            refused.add(client.impersonate("alice").toCompletableFuture());
        }
        time.set(TimeUnit.MILLISECONDS.toNanos(500));
        var probe = client.impersonate("alice").toCompletableFuture();
        var concurrentProbe = client.impersonate("bob").toCompletableFuture();

        // Then
        assertThat(first).isCompletedExceptionally();
        assertThat(refused).allMatch(CompletableFuture::isCompletedExceptionally);
        assertThat(exchanges).hasSize(1);
        assertThat(concurrentProbe).isCompletedExceptionally();
        exchanges.remove().complete(reply(403));
        assertThat(probe).isCompletedExceptionally();
        client.impersonate("alice");
        assertThat(exchanges).hasSize(1); // A principal-specific denial proves the endpoint is responding.
        exchanges.remove().complete(reply(403));
        client.close();
    }

    @Test
    void staleCompletionCannotReopenTheGateAfterAnOutage() {
        // Given
        client.impersonate("alice");
        client.impersonate("bob");
        exchanges.remove().complete(reply(503));

        // When
        exchanges.remove().complete(reply(403));
        var result = client.impersonate("alice").toCompletableFuture();

        // Then
        assertThatThrownBy(result::join).hasCauseInstanceOf(MdsFailure.class).hasMessageContaining("MDS_BACKOFF");
        assertThat(exchanges).isEmpty();
        client.close();
    }

    @SuppressWarnings("unchecked")
    private static HttpResponse<byte[]> reply(int status) {
        HttpResponse<byte[]> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        return response;
    }
}
