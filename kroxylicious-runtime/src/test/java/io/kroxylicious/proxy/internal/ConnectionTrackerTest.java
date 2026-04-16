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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class ConnectionTrackerTest {

    private static final String CLUSTER_A = "cluster-a";
    private static final String CLUSTER_B = "cluster-b";

    private ConnectionTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new ConnectionTracker();
    }

    // ── Downstream tests ──

    @Test
    void shouldTrackDownstreamConnection() {
        Channel ch = mock(Channel.class);

        tracker.onDownstreamConnectionEstablished(CLUSTER_A, ch);

        assertThat(tracker.getDownstreamActiveChannels(CLUSTER_A)).containsExactly(ch);
    }

    @Test
    void shouldRemoveDownstreamConnectionOnClose() {
        Channel ch = mock(Channel.class);
        tracker.onDownstreamConnectionEstablished(CLUSTER_A, ch);

        tracker.onDownstreamConnectionClosed(CLUSTER_A, ch);

        assertThat(tracker.getDownstreamActiveChannels(CLUSTER_A)).isEmpty();
    }

    @Test
    void shouldReturnEmptySetForUnknownCluster() {
        assertThat(tracker.getDownstreamActiveChannels("unknown")).isEmpty();
        assertThat(tracker.getUpstreamActiveChannels("unknown")).isEmpty();
    }

    @Test
    void shouldTrackMultipleChannelsPerCluster() {
        Channel ch1 = mock(Channel.class);
        Channel ch2 = mock(Channel.class);
        Channel ch3 = mock(Channel.class);

        tracker.onDownstreamConnectionEstablished(CLUSTER_A, ch1);
        tracker.onDownstreamConnectionEstablished(CLUSTER_A, ch2);
        tracker.onDownstreamConnectionEstablished(CLUSTER_A, ch3);

        assertThat(tracker.getDownstreamActiveChannels(CLUSTER_A)).containsExactlyInAnyOrder(ch1, ch2, ch3);
    }

    @Test
    void shouldTrackChannelsAcrossMultipleClusters() {
        Channel chA1 = mock(Channel.class);
        Channel chA2 = mock(Channel.class);
        Channel chB1 = mock(Channel.class);

        tracker.onDownstreamConnectionEstablished(CLUSTER_A, chA1);
        tracker.onDownstreamConnectionEstablished(CLUSTER_A, chA2);
        tracker.onDownstreamConnectionEstablished(CLUSTER_B, chB1);

        assertThat(tracker.getDownstreamActiveChannels(CLUSTER_A)).containsExactlyInAnyOrder(chA1, chA2);
        assertThat(tracker.getDownstreamActiveChannels(CLUSTER_B)).containsExactly(chB1);
    }

    @Test
    void shouldHandleDoubleClose() {
        Channel ch = mock(Channel.class);
        tracker.onDownstreamConnectionEstablished(CLUSTER_A, ch);

        tracker.onDownstreamConnectionClosed(CLUSTER_A, ch);
        tracker.onDownstreamConnectionClosed(CLUSTER_A, ch);

        assertThat(tracker.getDownstreamActiveChannels(CLUSTER_A)).isEmpty();
    }

    @Test
    void shouldReturnUnmodifiableSetForDownstream() {
        Channel ch = mock(Channel.class);
        tracker.onDownstreamConnectionEstablished(CLUSTER_A, ch);

        var activeChannels = tracker.getDownstreamActiveChannels(CLUSTER_A);

        assertThatThrownBy(() -> activeChannels.add(mock(Channel.class)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // ── Upstream tests ──

    @Test
    void shouldTrackUpstreamConnection() {
        Channel ch = mock(Channel.class);

        tracker.onUpstreamConnectionEstablished(CLUSTER_A, ch);

        assertThat(tracker.getUpstreamActiveChannels(CLUSTER_A)).containsExactly(ch);
    }

    @Test
    void shouldRemoveUpstreamConnectionOnClose() {
        Channel ch = mock(Channel.class);
        tracker.onUpstreamConnectionEstablished(CLUSTER_A, ch);

        tracker.onUpstreamConnectionClosed(CLUSTER_A, ch);

        assertThat(tracker.getUpstreamActiveChannels(CLUSTER_A)).isEmpty();
    }

    @Test
    void shouldReturnUnmodifiableSetForUpstream() {
        Channel ch = mock(Channel.class);
        tracker.onUpstreamConnectionEstablished(CLUSTER_A, ch);

        var activeChannels = tracker.getUpstreamActiveChannels(CLUSTER_A);

        assertThatThrownBy(() -> activeChannels.add(mock(Channel.class)))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
