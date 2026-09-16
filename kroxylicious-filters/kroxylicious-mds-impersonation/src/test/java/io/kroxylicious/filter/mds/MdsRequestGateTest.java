/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MdsRequestGateTest {
    @ParameterizedTest
    @CsvSource({ "1,750,0", "2,1250,0", "3,2250,0", "4,4250,0", "5,5000,0", "6,5000,0",
            "1,750,1", "2,1250,1", "3,2250,1", "4,4250,1", "5,5000,1", "6,5000,1" })
    void increasesBackoffUpToFiveSecondsIncludingJitter(int failures, long delay, long remainingNanos) {
        // Given
        var time = new AtomicLong();
        var gate = new MdsRequestGate(time::get, () -> 250);
        for (int i = 1; i < failures; i++) {
            gate.complete(gate.acquire(), true);
            time.addAndGet(TimeUnit.SECONDS.toNanos(5));
        }
        gate.complete(gate.acquire(), true);

        // When
        time.addAndGet(TimeUnit.MILLISECONDS.toNanos(delay) - remainingNanos);

        // Then
        if (remainingNanos == 0) {
            assertThat(gate.acquire().probe()).isTrue();
        }
        else {
            assertThatThrownBy(gate::acquire).hasMessageContaining("MDS_BACKOFF");
        }
    }

    @Test
    void aSuccessfulProbeResetsTheFailureCount() {
        // Given
        var time = new AtomicLong();
        var gate = new MdsRequestGate(time::get, () -> 0);
        gate.complete(gate.acquire(), true);
        time.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
        gate.complete(gate.acquire(), false);

        // When
        gate.complete(gate.acquire(), true);
        time.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));

        // Then
        assertThat(gate.acquire().probe()).isTrue();
    }

    @Test
    void closingRejectsRequestsEvenAfterOutstandingCallsFinish() {
        // Given
        var gate = new MdsRequestGate();
        var permit = gate.acquire();

        // When
        gate.close();
        gate.complete(permit, false);

        // Then
        assertThatThrownBy(gate::acquire).hasMessageContaining("MDS_CLOSED");
    }
}
