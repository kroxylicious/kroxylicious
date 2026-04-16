/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConnectionDrainManagerTest {

    private static final String CLUSTER_A = "cluster-a";

    private ConnectionTracker connectionTracker;
    private InFlightRequestTracker inFlightRequestTracker;
    private ConnectionDrainManager drainManager;

    private static Channel mockChannel(boolean active) {
        Channel ch = mock(Channel.class);
        ChannelConfig config = mock(ChannelConfig.class);
        when(ch.config()).thenReturn(config);
        when(ch.isActive()).thenReturn(active);
        when(ch.close()).thenReturn(mock(ChannelFuture.class));
        return ch;
    }

    @BeforeEach
    void setUp() {
        connectionTracker = new ConnectionTracker();
        inFlightRequestTracker = new InFlightRequestTracker();
        drainManager = new ConnectionDrainManager(connectionTracker, inFlightRequestTracker);
    }

    @AfterEach
    void tearDown() {
        drainManager.close();
    }

    @Test
    void shouldCompleteImmediatelyWhenNoActiveConnections() throws Exception {
        var future = drainManager.gracefullyDrainConnections(CLUSTER_A, Duration.ofSeconds(5));

        assertThat(future).succeedsWithin(Duration.ofSeconds(1));
    }

    @Test
    void shouldDisableAutoReadOnDownstreamChannels() throws Exception {
        Channel ch = mockChannel(true);
        connectionTracker.onDownstreamConnectionEstablished(CLUSTER_A, ch);

        drainManager.gracefullyDrainConnections(CLUSTER_A, Duration.ofSeconds(5));

        // autoRead should be disabled immediately
        verify(ch.config(), timeout(500)).setAutoRead(false);
    }

    @Test
    void shouldCloseDownstreamChannelWhenNoInFlightRequests() throws Exception {
        Channel ch = mockChannel(true);
        connectionTracker.onDownstreamConnectionEstablished(CLUSTER_A, ch);
        // No in-flight requests — pending count is 0

        var future = drainManager.gracefullyDrainConnections(CLUSTER_A, Duration.ofSeconds(5));

        // Channel should be closed quickly (within poll interval)
        verify(ch, timeout(1000)).close();
        assertThat(future).succeedsWithin(Duration.ofSeconds(2));
    }

    @Test
    void shouldWaitForInFlightBeforeClosing() throws Exception {
        Channel ch = mockChannel(true);
        connectionTracker.onDownstreamConnectionEstablished(CLUSTER_A, ch);
        inFlightRequestTracker.onRequestSent(CLUSTER_A, ch);

        var future = drainManager.gracefullyDrainConnections(CLUSTER_A, Duration.ofSeconds(5));

        // Should NOT be closed yet — has in-flight request
        Thread.sleep(300);
        verify(ch, never()).close();

        // Simulate response received
        inFlightRequestTracker.onResponseReceived(CLUSTER_A, ch);

        // Now should close
        verify(ch, timeout(1000)).close();
        assertThat(future).succeedsWithin(Duration.ofSeconds(2));
    }

    @Test
    void shouldForceCloseAfterTimeout() throws Exception {
        Channel ch = mockChannel(true);
        connectionTracker.onDownstreamConnectionEstablished(CLUSTER_A, ch);
        inFlightRequestTracker.onRequestSent(CLUSTER_A, ch);
        // Never send response — force-close should trigger

        var future = drainManager.gracefullyDrainConnections(CLUSTER_A, Duration.ofMillis(500));

        // Should force-close after timeout
        verify(ch, timeout(2000)).close();
        assertThat(future).succeedsWithin(Duration.ofSeconds(3));
    }

    @Test
    void shouldCloseUpstreamAfterAllDownstreamClosed() throws Exception {
        Channel downstream = mockChannel(true);
        Channel upstream = mockChannel(true);
        connectionTracker.onDownstreamConnectionEstablished(CLUSTER_A, downstream);
        connectionTracker.onUpstreamConnectionEstablished(CLUSTER_A, upstream);
        // No in-flight — downstream closes immediately

        var future = drainManager.gracefullyDrainConnections(CLUSTER_A, Duration.ofSeconds(5));

        // Both should be closed
        verify(downstream, timeout(1000)).close();
        verify(upstream, timeout(1000)).close();
        assertThat(future).succeedsWithin(Duration.ofSeconds(2));
    }

    @Test
    void shouldCompleteTheFuture() throws Exception {
        Channel ch = mockChannel(true);
        connectionTracker.onDownstreamConnectionEstablished(CLUSTER_A, ch);

        var future = drainManager.gracefullyDrainConnections(CLUSTER_A, Duration.ofSeconds(5));

        assertThat(future).succeedsWithin(Duration.ofSeconds(2));
        assertThat(future.isDone()).isTrue();
        assertThat(future.isCompletedExceptionally()).isFalse();
    }

    @Test
    void shouldHandleCloseGracefully() {
        // Just verify close doesn't throw
        drainManager.close();
        // Second close should also be safe
        drainManager.close();
    }
}
