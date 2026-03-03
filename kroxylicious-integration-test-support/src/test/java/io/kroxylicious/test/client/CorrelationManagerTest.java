/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.client;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationManagerTest {

    @Test
    void testPendingFuturesCompletedExceptionallyOnChannelClose() {
        // given
        CorrelationManager correlationManager = new CorrelationManager();
        CompletableFuture<SequencedResponse> responseFuture = new CompletableFuture<>();
        CompletableFuture<SequencedResponse> responseFuture2 = new CompletableFuture<>();
        correlationManager.putBrokerRequest((short) 1, (short) 1, 1, responseFuture, (short) 1);
        correlationManager.putBrokerRequest((short) 1, (short) 1, 2, responseFuture2, (short) 1);

        // when
        correlationManager.onChannelClose();

        // then
        assertThat(responseFuture).failsWithin(Duration.ZERO);
        assertThat(responseFuture2).failsWithin(Duration.ZERO);
    }

    @Test
    void testNullFutureToleratedOnChannelClose() {
        // given
        CorrelationManager correlationManager = new CorrelationManager();
        CompletableFuture<SequencedResponse> responseFuture = new CompletableFuture<>();
        correlationManager.putBrokerRequest((short) 1, (short) 1, 1, responseFuture, (short) 1);
        correlationManager.putBrokerRequest((short) 1, (short) 1, 2, null, (short) 1);

        // when
        correlationManager.onChannelClose();

        // then
        assertThat(responseFuture).failsWithin(Duration.ZERO);
    }

    @Test
    void testCorrelationRetrievableOnceOnly() {
        // given
        int correlationId = 1;
        CorrelationManager correlationManager = new CorrelationManager();
        CompletableFuture<SequencedResponse> responseFuture = new CompletableFuture<>();
        correlationManager.putBrokerRequest((short) 1, (short) 1, correlationId, responseFuture, (short) 1);
        CorrelationManager.Correlation brokerCorrelation = correlationManager.getBrokerCorrelation(correlationId);
        assertThat(brokerCorrelation).isNotNull();

        // when
        CorrelationManager.Correlation brokerCorrelation2 = correlationManager.getBrokerCorrelation(correlationId);

        // then
        assertThat(brokerCorrelation2).isNull();
    }

}
