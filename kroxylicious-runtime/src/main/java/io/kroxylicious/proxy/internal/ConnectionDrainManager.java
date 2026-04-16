/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

/**
 * Orchestrates graceful connection draining for virtual clusters.
 * <p>
 * When a cluster is draining:
 * <ol>
 *   <li>autoRead is disabled on all downstream channels (no new requests from clients)</li>
 *   <li>Upstream channels keep autoRead=true so responses to in-flight requests can flow back</li>
 *   <li>Each downstream channel is closed when its in-flight request count reaches 0</li>
 *   <li>After all downstream channels are closed, upstream channels are closed</li>
 *   <li>After the timeout, any remaining channels (both downstream and upstream) are force-closed</li>
 * </ol>
 * <p>
 * Uses its own {@link ScheduledExecutorService} for polling — does NOT use Netty event loops.
 * Channel operations ({@code setAutoRead}, {@code close}) are dispatched onto each channel's event loop.
 */
public class ConnectionDrainManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionDrainManager.class);
    private static final long POLL_INTERVAL_MS = 100;

    private final ConnectionTracker connectionTracker;
    private final InFlightRequestTracker inFlightRequestTracker;
    private final ScheduledExecutorService scheduler;

    public ConnectionDrainManager(ConnectionTracker connectionTracker,
                                  InFlightRequestTracker inFlightRequestTracker) {
        this.connectionTracker = connectionTracker;
        this.inFlightRequestTracker = inFlightRequestTracker;
        var executor = new ScheduledThreadPoolExecutor(2, r -> {
            Thread t = new Thread(r, "connection-drain-manager");
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        this.scheduler = executor;
    }

    /**
     * Gracefully drains all connections for the given cluster.
     *
     * @param clusterName the virtual cluster to drain
     * @param timeout maximum time to wait for in-flight requests to complete
     * @return future that completes when all connections are closed (or timeout)
     */
    public CompletableFuture<Void> gracefullyDrainConnections(String clusterName, Duration timeout) {
        Set<Channel> downstreamChannels = connectionTracker.getDownstreamActiveChannels(clusterName);
        Set<Channel> upstreamChannels = connectionTracker.getUpstreamActiveChannels(clusterName);

        int totalDownstream = downstreamChannels.size();
        int totalUpstream = upstreamChannels.size();
        int totalInFlight = inFlightRequestTracker.getTotalPendingRequestCount(clusterName);

        LOGGER.atInfo()
                .addKeyValue("virtualCluster", clusterName)
                .addKeyValue("downstreamConnections", totalDownstream)
                .addKeyValue("upstreamConnections", totalUpstream)
                .addKeyValue("inFlightRequests", totalInFlight)
                .log("Starting graceful connection drain");

        if (totalDownstream == 0 && totalUpstream == 0) {
            LOGGER.atInfo()
                    .addKeyValue("virtualCluster", clusterName)
                    .log("No active connections to drain");
            return CompletableFuture.completedFuture(null);
        }

        // Step 1: Disable autoRead on all downstream channels
        List<Channel> downstreamSnapshot = new ArrayList<>(downstreamChannels);
        for (Channel ch : downstreamSnapshot) {
            if (ch.isActive()) {
                ch.config().setAutoRead(false);
            }
        }

        // Step 2: Schedule polling + timeout
        CompletableFuture<Void> drainFuture = new CompletableFuture<>();
        Instant deadline = Instant.now().plus(timeout);

        ScheduledFuture<?> pollTask = scheduler.scheduleAtFixedRate(() -> {
            try {
                pollAndCloseCompletedChannels(clusterName, downstreamSnapshot, deadline, drainFuture);
            }
            catch (Exception e) {
                LOGGER.atWarn()
                        .addKeyValue("virtualCluster", clusterName)
                        .addKeyValue("error", e.getMessage())
                        .log("Error during drain polling");
            }
        }, POLL_INTERVAL_MS, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // Cancel the poll task when the future completes
        drainFuture.whenComplete((v, t) -> pollTask.cancel(false));

        return drainFuture;
    }

    private void pollAndCloseCompletedChannels(String clusterName,
                                               List<Channel> downstreamSnapshot,
                                               Instant deadline,
                                               CompletableFuture<Void> drainFuture) {
        if (drainFuture.isDone()) {
            return;
        }

        boolean timedOut = Instant.now().isAfter(deadline);
        boolean allDownstreamClosed = true;

        for (Channel ch : downstreamSnapshot) {
            if (!ch.isActive()) {
                continue;
            }

            int pending = inFlightRequestTracker.getPendingRequestCount(clusterName, ch);

            if (pending <= 0 || timedOut) {
                if (timedOut && pending > 0) {
                    LOGGER.atWarn()
                            .addKeyValue("virtualCluster", clusterName)
                            .addKeyValue("channel", ch)
                            .addKeyValue("pendingRequests", pending)
                            .log("Force-closing downstream channel — drain timeout reached");
                }
                else {
                    LOGGER.atDebug()
                            .addKeyValue("virtualCluster", clusterName)
                            .addKeyValue("channel", ch)
                            .log("Closing downstream channel — all in-flight requests completed");
                }
                ch.close();
            }
            else {
                allDownstreamClosed = false;
            }
        }

        if (allDownstreamClosed || timedOut) {
            // Close all upstream channels
            Set<Channel> upstreamChannels = connectionTracker.getUpstreamActiveChannels(clusterName);
            for (Channel ch : upstreamChannels) {
                if (ch.isActive()) {
                    if (timedOut) {
                        LOGGER.atWarn()
                                .addKeyValue("virtualCluster", clusterName)
                                .addKeyValue("channel", ch)
                                .log("Force-closing upstream channel");
                    }
                    ch.close();
                }
            }

            int totalClosed = downstreamSnapshot.size() + upstreamChannels.size();
            LOGGER.atInfo()
                    .addKeyValue("virtualCluster", clusterName)
                    .addKeyValue("totalClosed", totalClosed)
                    .addKeyValue("timedOut", timedOut)
                    .log("Connection drain complete");

            drainFuture.complete(null);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.atWarn().log("Drain manager scheduler did not terminate cleanly");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
