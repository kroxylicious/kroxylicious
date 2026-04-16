/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.Channel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class InFlightRequestTrackerTest {

    private static final String CLUSTER_A = "cluster-a";
    private static final String CLUSTER_B = "cluster-b";

    private InFlightRequestTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new InFlightRequestTracker();
    }

    @Test
    void shouldTrackRequestSent() {
        Channel ch = mock(Channel.class);

        tracker.onRequestSent(CLUSTER_A, ch);

        assertThat(tracker.getPendingRequestCount(CLUSTER_A, ch)).isEqualTo(1);
        assertThat(tracker.getTotalPendingRequestCount(CLUSTER_A)).isEqualTo(1);
    }

    @Test
    void shouldDecrementOnResponseReceived() {
        Channel ch = mock(Channel.class);
        tracker.onRequestSent(CLUSTER_A, ch);
        tracker.onRequestSent(CLUSTER_A, ch);

        tracker.onResponseReceived(CLUSTER_A, ch);

        assertThat(tracker.getPendingRequestCount(CLUSTER_A, ch)).isEqualTo(1);
        assertThat(tracker.getTotalPendingRequestCount(CLUSTER_A)).isEqualTo(1);
    }

    @Test
    void shouldReachZeroAfterAllResponsesReceived() {
        Channel ch = mock(Channel.class);
        tracker.onRequestSent(CLUSTER_A, ch);
        tracker.onRequestSent(CLUSTER_A, ch);
        tracker.onRequestSent(CLUSTER_A, ch);

        tracker.onResponseReceived(CLUSTER_A, ch);
        tracker.onResponseReceived(CLUSTER_A, ch);
        tracker.onResponseReceived(CLUSTER_A, ch);

        assertThat(tracker.getPendingRequestCount(CLUSTER_A, ch)).isZero();
        assertThat(tracker.getTotalPendingRequestCount(CLUSTER_A)).isZero();
    }

    @Test
    void shouldNotGoNegative() {
        Channel ch = mock(Channel.class);

        // Receive without send
        tracker.onResponseReceived(CLUSTER_A, ch);

        assertThat(tracker.getPendingRequestCount(CLUSTER_A, ch)).isZero();
        assertThat(tracker.getTotalPendingRequestCount(CLUSTER_A)).isZero();
    }

    @Test
    void shouldTrackTotalPendingByCluster() {
        Channel ch1 = mock(Channel.class);
        Channel ch2 = mock(Channel.class);

        tracker.onRequestSent(CLUSTER_A, ch1);
        tracker.onRequestSent(CLUSTER_A, ch1);
        tracker.onRequestSent(CLUSTER_A, ch2);
        tracker.onRequestSent(CLUSTER_A, ch2);

        assertThat(tracker.getPendingRequestCount(CLUSTER_A, ch1)).isEqualTo(2);
        assertThat(tracker.getPendingRequestCount(CLUSTER_A, ch2)).isEqualTo(2);
        assertThat(tracker.getTotalPendingRequestCount(CLUSTER_A)).isEqualTo(4);
    }

    @Test
    void shouldCleanUpOnChannelClosed() {
        Channel ch = mock(Channel.class);
        tracker.onRequestSent(CLUSTER_A, ch);
        tracker.onRequestSent(CLUSTER_A, ch);
        tracker.onRequestSent(CLUSTER_A, ch);

        tracker.onChannelClosed(CLUSTER_A, ch);

        assertThat(tracker.getPendingRequestCount(CLUSTER_A, ch)).isZero();
        assertThat(tracker.getTotalPendingRequestCount(CLUSTER_A)).isZero();
    }

    @Test
    void shouldReturnZeroForUnknownCluster() {
        Channel ch = mock(Channel.class);

        assertThat(tracker.getPendingRequestCount("unknown", ch)).isZero();
        assertThat(tracker.getTotalPendingRequestCount("unknown")).isZero();
    }

    @Test
    void shouldReturnZeroForUnknownChannel() {
        Channel known = mock(Channel.class);
        Channel unknown = mock(Channel.class);
        tracker.onRequestSent(CLUSTER_A, known);

        assertThat(tracker.getPendingRequestCount(CLUSTER_A, unknown)).isZero();
    }

    @Test
    void shouldHandleMultipleClustersIndependently() {
        Channel chA = mock(Channel.class);
        Channel chB = mock(Channel.class);

        tracker.onRequestSent(CLUSTER_A, chA);
        tracker.onRequestSent(CLUSTER_A, chA);
        tracker.onRequestSent(CLUSTER_B, chB);

        assertThat(tracker.getTotalPendingRequestCount(CLUSTER_A)).isEqualTo(2);
        assertThat(tracker.getTotalPendingRequestCount(CLUSTER_B)).isEqualTo(1);

        tracker.onResponseReceived(CLUSTER_A, chA);

        assertThat(tracker.getTotalPendingRequestCount(CLUSTER_A)).isEqualTo(1);
        assertThat(tracker.getTotalPendingRequestCount(CLUSTER_B)).isEqualTo(1);
    }
}
