/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.codec;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.RequestResponseState;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationManagerTest {

    private final short apiKey = (short) 1;
    private final short apiVersion = (short) 2;
    private final int downstreamCorrelationId = 3;
    private final Filter recipient = Mockito.mock(Filter.class);
    private final CompletableFuture<Object> promise = new CompletableFuture<>();
    private final RequestResponseState state = Mockito.mock(RequestResponseState.class);
    private final boolean decodeResponse = true;

    @Test
    public void hasResponse() {
        CorrelationManager correlationManager = new CorrelationManager();
        int upstreamCorrelationId = correlationManager.putBrokerRequest(apiKey, apiVersion, downstreamCorrelationId, true, recipient, promise, decodeResponse, state);
        CorrelationManager.Correlation correlation = correlationManager.getBrokerCorrelation(upstreamCorrelationId);
        assertThat(correlation).isNotNull();
        assertThat(correlation.apiKey()).isEqualTo(apiKey);
        assertThat(correlation.apiVersion()).isEqualTo(apiVersion);
        assertThat(correlation.downstreamCorrelationId()).isEqualTo(downstreamCorrelationId);
        assertThat(correlation.recipient()).isSameAs(recipient);
        assertThat(correlation.promise()).isSameAs(promise);
        assertThat(correlation.state()).isSameAs(state);
    }

    @Test
    public void correlationRemovedOnGet() {
        CorrelationManager correlationManager = new CorrelationManager();
        int upstreamCorrelationId = correlationManager.putBrokerRequest(apiKey, apiVersion, downstreamCorrelationId, true, recipient, promise, decodeResponse, state);
        // removes correlation
        correlationManager.getBrokerCorrelation(upstreamCorrelationId);

        CorrelationManager.Correlation correlation = correlationManager.getBrokerCorrelation(upstreamCorrelationId);
        assertThat(correlation).isNull();
    }

    @Test
    public void hasNoResponse() {
        CorrelationManager correlationManager = new CorrelationManager();
        int upstreamCorrelationId = correlationManager.putBrokerRequest(apiKey, apiVersion, downstreamCorrelationId, false, recipient, promise, decodeResponse, state);
        CorrelationManager.Correlation correlation = correlationManager.getBrokerCorrelation(upstreamCorrelationId);
        assertThat(correlation).isNull();
    }

}
