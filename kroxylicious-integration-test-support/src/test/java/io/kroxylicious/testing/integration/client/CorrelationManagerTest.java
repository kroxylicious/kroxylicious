/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.client;

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
    void correlationEqualityRespectsApiFieldsAndIgnoresResponseFuture() {
        // Given
        var future1 = new CompletableFuture<SequencedResponse>();
        var future2 = new CompletableFuture<SequencedResponse>();
        var baseline = new CorrelationManager.Correlation((short) 1, (short) 2, future1, (short) 3);
        var sameFieldsDifferentFuture = new CorrelationManager.Correlation((short) 1, (short) 2, future2, (short) 3);

        // Then
        assertThat(baseline)
                .isNotEqualTo(new Object())
                .isNotEqualTo(null);

        assertThat(baseline)
                .isEqualTo(sameFieldsDifferentFuture)
                .hasSameHashCodeAs(sameFieldsDifferentFuture);
        assertThat(sameFieldsDifferentFuture).isEqualTo(baseline);

        assertThat(baseline)
                .isNotEqualTo(new CorrelationManager.Correlation((short) 9, (short) 2, future1, (short) 3))
                .isNotEqualTo(new CorrelationManager.Correlation((short) 1, (short) 9, future1, (short) 3))
                .isNotEqualTo(new CorrelationManager.Correlation((short) 1, (short) 2, future1, (short) 9));
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
