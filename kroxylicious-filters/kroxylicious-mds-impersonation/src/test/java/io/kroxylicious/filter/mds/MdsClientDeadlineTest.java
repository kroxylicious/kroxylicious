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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class MdsClientDeadlineTest {
    private final HttpClient http = mock(HttpClient.class);
    private final AtomicLong time = new AtomicLong();
    private final MdsRequestGate gate = new MdsRequestGate(time::get, () -> 0);
    private final MdsImpersonationConfig config = new MdsImpersonationConfig(URI.create("https://mds/security/1.0/impersonate"),
            new Tls(new KeyPair("key", "cert", null), null, null, null), Duration.ofMillis(1), Duration.ofSeconds(5));

    @Test
    void aStalledBodyTimesOutAndCancelsTheExchange() {
        // Given
        var exchange = new CompletableFuture<HttpResponse<byte[]>>();
        doReturn(exchange).when(http).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
        try (var client = new MdsClient(http, config, gate)) {
            // When
            var result = client.impersonate("alice").toCompletableFuture();

            // Then
            assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(MdsFailure.class).hasMessageContaining("MDS_TIMEOUT");
            assertThat(exchange).isCancelled();
        }
    }

    @Test
    void synchronousExecutorRejectionReleasesThePermitAndStartsBackoff() {
        // Given
        doThrow(new RejectedExecutionException("sensitive-text")).when(http).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
        try (var client = new MdsClient(http, config, gate)) {
            // When
            var result = client.impersonate("alice").toCompletableFuture();
            time.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));

            // Then
            assertThatThrownBy(result::join).hasCauseInstanceOf(MdsFailure.class).hasMessageContaining("MDS_CAPACITY")
                    .hasMessageNotContaining("sensitive-text");
            assertThat(gate.acquire().probe()).isTrue();
        }
    }
}
