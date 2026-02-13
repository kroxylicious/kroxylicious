/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

/**
 * Manages graceful connection draining for virtual clusters.
 * When a virtual cluster is marked for draining, no new connections will be accepted,
 * and existing connections will be allowed to complete their current operations before being closed.
 */
public class ConnectionDrainManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionDrainManager.class);

    private final ConnectionTracker connectionTracker;
    private final InFlightMessageTracker inFlightTracker;
    private final Map<String, AtomicBoolean> drainingClusters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    /**
     * Creates a new ConnectionDrainManager.
     *
     * @param connectionTracker the connection tracker to monitor active connections
     * @param inFlightTracker the tracker for in-flight messages
     */
    public ConnectionDrainManager(ConnectionTracker connectionTracker,
                                  InFlightMessageTracker inFlightTracker) {
        this.connectionTracker = connectionTracker;
        this.inFlightTracker = inFlightTracker;
        this.scheduler = new ScheduledThreadPoolExecutor(2, r -> {
            Thread t = new Thread(r, "connection-drain-manager");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Checks if the specified virtual cluster is currently draining connections.
     *
     * @param clusterName the name of the virtual cluster
     * @return true if the cluster is draining, false otherwise
     */
    public boolean isDraining(String clusterName) {
        AtomicBoolean draining = drainingClusters.get(clusterName);
        return draining != null && draining.get();
    }

    /**
     * Stops draining for the specified virtual cluster, allowing new connections to be accepted.
     *
     * @param clusterName the name of the virtual cluster
     */
    public void stopDraining(String clusterName) {
        drainingClusters.remove(clusterName);
        LOGGER.info("Stopped draining for virtual cluster '{}'", clusterName);
    }

    /**
     * Determines if a new connection should be accepted for the specified virtual cluster.
     *
     * @param clusterName the name of the virtual cluster
     * @return true if the connection should be accepted, false if it should be rejected
     */
    public boolean shouldAcceptConnection(String clusterName) {
        return !isDraining(clusterName);
    }

    /**
     * Performs a complete graceful drain operation by stopping new connections
     * and immediately closing existing connections after in-flight messages complete.
     *
     * Kafka clients typically maintain persistent connections and won't close them
     * naturally, so we must actively close them after ensuring message delivery.
     *
     * @param clusterName the name of the virtual cluster
     * @param totalTimeout the total time allowed for the entire drain operation
     * @return a CompletableFuture that completes when draining is finished
     */
    public CompletableFuture<Void> gracefullyDrainConnections(String clusterName, Duration totalTimeout) {

        int totalActiveConnections = connectionTracker.getTotalConnectionCount(clusterName);
        int totalInFlightRequests = inFlightTracker.getTotalPendingRequestCount(clusterName);
        LOGGER.info("Starting graceful drain for cluster '{}' with {} total active connections and {} in-flight requests ({}s timeout)",
                clusterName, totalActiveConnections, totalInFlightRequests, totalTimeout.getSeconds());

        return startDraining(clusterName)
                .thenCompose(v -> {
                    if (totalActiveConnections == 0) {
                        LOGGER.info("No active connections for cluster '{}', drain complete", clusterName);
                        return CompletableFuture.completedFuture(null);
                    }
                    else {
                        // Immediately start gracefully closing connections after in-flight messages
                        return gracefullyCloseConnections(clusterName, totalTimeout);
                    }
                });
    }

    /**
     * Starts draining connections for the specified virtual cluster.
     * Once draining is started, new connections will be rejected.
     *
     * @param clusterName the name of the virtual cluster to drain
     * @return a CompletableFuture that completes when draining has started
     */
    public CompletableFuture<Void> startDraining(String clusterName) {
        drainingClusters.put(clusterName, new AtomicBoolean(true));
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Gracefully closes all active connections for the specified virtual cluster.
     * Strategy: Disable autoRead on downstream channels to prevent new requests,
     * but keep upstream channels reading to allow responses to complete naturally.
     *
     * @param clusterName the name of the virtual cluster
     * @param timeout the maximum time to wait for graceful closure
     * @return a CompletableFuture that completes when all connections are closed
     */
    public CompletableFuture<Void> gracefullyCloseConnections(String clusterName, Duration timeout) {

        // Get both downstream (client-to-proxy) and upstream (proxy-to-Kafka) connections
        Set<Channel> downstreamChannels = connectionTracker.getDownstreamActiveChannels(clusterName);
        Set<Channel> upstreamChannels = connectionTracker.getUpstreamActiveChannels(clusterName);

        if (downstreamChannels.isEmpty() && upstreamChannels.isEmpty()) {
            LOGGER.debug("No active connections to close for cluster '{}'", clusterName);
            return CompletableFuture.completedFuture(null);
        }

        int totalConnections = downstreamChannels.size() + upstreamChannels.size();
        int totalInFlight = inFlightTracker.getTotalPendingRequestCount(clusterName);

        LOGGER.info("Gracefully closing {} total connections for cluster '{}' (downstream: {}, upstream: {}) with {} in-flight requests",
                totalConnections, clusterName, downstreamChannels.size(), upstreamChannels.size(), totalInFlight);

        // Close both downstream and upstream channels gracefully
        var allCloseFutures = new ArrayList<CompletableFuture<Void>>();

        // STRATEGY: Disable autoRead ONLY on downstream channels (via Backpressure)
        // - Downstream (autoRead=false): Prevents new client requests from being processed
        // - Upstream (autoRead=true): Allows Kafka responses to be processed normally
        // - Result: In-flight counts should decrease naturally to zero or wait until timeout and then force close the channel

        // Add downstream channel close futures
        downstreamChannels.stream()
                .map(this::disableAutoReadOnDownstreamChannel)
                .map(channel -> gracefullyCloseChannel(channel, clusterName, timeout, "DOWNSTREAM"))
                .forEach(allCloseFutures::add);

        // Add upstream channel close futures
        upstreamChannels.stream()
                .map(channel -> gracefullyCloseChannel(channel, clusterName, timeout, "UPSTREAM"))
                .forEach(allCloseFutures::add);

        return CompletableFuture.allOf(allCloseFutures.toArray(new CompletableFuture[0]))
                .whenComplete((result, throwable) -> {
                    int remainingInFlight = inFlightTracker.getTotalPendingRequestCount(clusterName);
                    if (throwable != null) {
                        LOGGER.error("Error during graceful connection closure for cluster '{}' with {} remaining in-flight requests",
                                clusterName, remainingInFlight, throwable);
                    }
                    else {
                        LOGGER.info("Completed graceful closure of {} total connections for cluster '{}' with {} remaining in-flight requests",
                                totalConnections, clusterName, remainingInFlight);
                    }
                });
    }

    /**
     * Disables autoRead on downstream channel to prevent new client requests during draining.
     * Upstream channels keep autoRead=true to allow Kafka responses to be processed.
     *
     * @param downstreamChannel the client-facing channels to disable autoRead on
     */
    private Channel disableAutoReadOnDownstreamChannel(Channel downstreamChannel) {
        try {
            if (downstreamChannel.isActive()) {
                // Get the KafkaProxyFrontendHandler from the channel pipeline
                KafkaProxyFrontendHandler frontendHandler = downstreamChannel.pipeline().get(KafkaProxyFrontendHandler.class);
                if (frontendHandler != null) {
                    frontendHandler.applyBackpressure();
                    LOGGER.debug("Applied backpressure via frontend handler for channel: L:/{}, R:/{}",
                            downstreamChannel.localAddress(), downstreamChannel.remoteAddress());
                }
                else {
                    LOGGER.debug("Manually applying backpressure for channel: L:/{}, R:/{}",
                            downstreamChannel.localAddress(), downstreamChannel.remoteAddress());
                    // Fallback to manual method if handler not found
                    downstreamChannel.config().setAutoRead(false);
                }
            }
        }
        catch (Exception e) {
            LOGGER.warn("Failed to disable autoRead for downstream channel L:/{}, R:/{} - continuing with drain",
                    downstreamChannel.localAddress(), downstreamChannel.remoteAddress(), e);
        }
        return downstreamChannel;
    }

    /**
     * Gracefully closes a single channel using a timeout-based approach.
     * Gives existing in-flight messages a brief grace period to complete, then closes the channel.
     * This avoids the paradox of autoRead=false blocking responses vs autoRead=true allowing new requests.
     *
     * @param channel the channel to close
     * @param clusterName the name of the virtual cluster (for logging and tracking)
     * @param timeout the maximum time to wait for graceful closure
     * @param channelType the type of channel (DOWNSTREAM or UPSTREAM) for logging
     * @return a CompletableFuture that completes when the channel is closed
     */
    private CompletableFuture<Void> gracefullyCloseChannel(Channel channel, String clusterName, Duration timeout, String channelType) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        long timeoutMillis = timeout.toMillis();
        long startTime = System.currentTimeMillis();

        LOGGER.info("Initiating graceful shutdown of {} channel L:/{}, R:/{} for cluster '{}'",
                channelType, channel.localAddress(), channel.remoteAddress(), clusterName);

        // Schedule timeout
        ScheduledFuture<?> timeoutTask = scheduler.schedule(() -> {
            if (!future.isDone()) {
                LOGGER.warn("Graceful shutdown timeout exceeded for {} channel L:/{}, R:/{} in cluster '{}' - forcing immediate closure",
                        channelType, channel.localAddress(), channel.remoteAddress(), clusterName);
                closeChannelImmediately(channel, future);
            }
        }, timeoutMillis, TimeUnit.MILLISECONDS);

        // Schedule periodic checks for in-flight messages
        ScheduledFuture<?> checkTask = scheduler.scheduleAtFixedRate(() -> {
            try {
                if (future.isDone()) {
                    return;
                }

                int pendingRequests = inFlightTracker.getPendingRequestCount(clusterName, channel);
                long elapsed = System.currentTimeMillis() - startTime;

                if (pendingRequests == 0) {
                    LOGGER.info("In-flight messages cleared for {} channel L:/{}, R:/{} in cluster '{}' - proceeding with connection closure ({}ms elapsed)",
                            channelType, channel.localAddress(), channel.remoteAddress(), clusterName, elapsed);
                    closeChannelImmediately(channel, future);
                }
                else {
                    // Just wait for existing in-flight messages to complete naturally
                    // Do NOT call channel.read() as it would trigger processing of new messages
                    int totalPending = inFlightTracker.getTotalPendingRequestCount(clusterName);
                    LOGGER.debug("Waiting for {} channel L:/{}, R:/{} in cluster '{}' to drain: {} pending requests (cluster total: {}, {}ms elapsed)",
                            channelType, channel.localAddress(), channel.remoteAddress(), clusterName, pendingRequests, totalPending, elapsed);
                }
            }
            catch (Exception e) {
                LOGGER.error("Unexpected error during graceful shutdown monitoring for channel L:/{}, R:/{} in cluster '{}'",
                        channel.localAddress(), channel.remoteAddress(), clusterName, e);
                future.completeExceptionally(e);
            }
        }, 50, 100, TimeUnit.MILLISECONDS); // Check every 100ms for faster response

        // Cancel scheduled tasks when future completes and log final result
        future.whenComplete((result, throwable) -> {
            timeoutTask.cancel(false);
            checkTask.cancel(false);

            if (throwable == null) {
                LOGGER.info("Successfully completed graceful shutdown of {} channel L:/{}, R:/{} in cluster '{}'",
                        channelType, channel.localAddress(), channel.remoteAddress(), clusterName);
            }
            else {
                LOGGER.error("Graceful shutdown failed for {} channel L:/{}, R:/{} in cluster '{}': {}",
                        channelType, channel.localAddress(), channel.remoteAddress(), clusterName, throwable.getMessage());
            }
        });

        return future;
    }

    /**
     * Immediately closes a channel and completes the associated future.
     */
    private void closeChannelImmediately(Channel channel, CompletableFuture<Void> future) {
        if (future.isDone()) {
            return;
        }

        channel.close().addListener(channelFuture -> {
            if (channelFuture.isSuccess()) {
                future.complete(null);
            }
            else {
                future.completeExceptionally(channelFuture.cause());
            }
        });
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
