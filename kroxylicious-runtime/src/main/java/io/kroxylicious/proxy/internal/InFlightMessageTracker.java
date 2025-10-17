/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

/**
 * Tracks in-flight Kafka requests and responses per virtual cluster and channel.
 * This enables graceful connection draining by ensuring all pending messages
 * are delivered before closing connections.
 * <p>
 * This class is thread-safe.
 */
public class InFlightMessageTracker {

    // Map from cluster name to channel to pending request count
    private final Map<String, Map<Channel, AtomicInteger>> pendingRequests = new ConcurrentHashMap<>();

    // Map from cluster name to total pending requests for quick lookup
    private final Map<String, AtomicInteger> totalPendingByCluster = new ConcurrentHashMap<>();

    /**
     * Records that a request has been sent to the upstream cluster.
     *
     * @param clusterName The name of the virtual cluster.
     * @param channel The channel handling the request.
     */
    public void onRequestSent(String clusterName, Channel channel) {
        pendingRequests.computeIfAbsent(clusterName, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(channel, k -> new AtomicInteger(0))
                .incrementAndGet();

        totalPendingByCluster.computeIfAbsent(clusterName, k -> new AtomicInteger(0))
                .incrementAndGet();
    }

    /**
     * Records that a response has been received from the upstream cluster.
     *
     * @param clusterName The name of the virtual cluster.
     * @param channel The channel handling the response.
     */
    public void onResponseReceived(String clusterName, Channel channel) {
        Map<Channel, AtomicInteger> clusterRequests = pendingRequests.get(clusterName);
        if (clusterRequests != null) {
            AtomicInteger channelCounter = clusterRequests.get(channel);
            if (channelCounter != null) {
                int remaining = channelCounter.decrementAndGet();
                if (remaining <= 0) {
                    clusterRequests.remove(channel);
                    if (clusterRequests.isEmpty()) {
                        pendingRequests.remove(clusterName);
                    }
                }

                AtomicInteger totalCounter = totalPendingByCluster.get(clusterName);
                if (totalCounter != null) {
                    int totalRemaining = totalCounter.decrementAndGet();
                    if (totalRemaining <= 0) {
                        totalPendingByCluster.remove(clusterName);
                    }
                }
            }
        }
    }

    /**
     * Records that a channel has been closed, clearing all pending requests for that channel.
     *
     * @param clusterName The name of the virtual cluster.
     * @param channel The channel that was closed.
     */
    public void onChannelClosed(String clusterName, Channel channel) {
        Map<Channel, AtomicInteger> clusterRequests = pendingRequests.get(clusterName);
        if (clusterRequests != null) {
            AtomicInteger channelCounter = clusterRequests.remove(channel);
            if (channelCounter != null) {
                int pendingCount = channelCounter.get();
                if (pendingCount > 0) {
                    // Subtract from total
                    AtomicInteger totalCounter = totalPendingByCluster.get(clusterName);
                    if (totalCounter != null) {
                        int newTotal = totalCounter.addAndGet(-pendingCount);
                        if (newTotal <= 0) {
                            totalPendingByCluster.remove(clusterName);
                        }
                    }
                }
            }

            if (clusterRequests.isEmpty()) {
                pendingRequests.remove(clusterName);
            }
        }
    }

    /**
     * Gets the number of pending requests for a specific channel in a virtual cluster.
     *
     * @param clusterName The name of the virtual cluster.
     * @param channel The channel.
     * @return The number of pending requests.
     */
    public int getPendingRequestCount(String clusterName, Channel channel) {
        Map<Channel, AtomicInteger> clusterRequests = pendingRequests.get(clusterName);
        if (clusterRequests != null) {
            AtomicInteger counter = clusterRequests.get(channel);
            return counter != null ? Math.max(0, counter.get()) : 0;
        }
        return 0;
    }

    /**
     * Gets the total number of pending requests for a virtual cluster across all channels.
     *
     * @param clusterName The name of the virtual cluster.
     * @return The total number of pending requests.
     */
    public int getTotalPendingRequestCount(String clusterName) {
        AtomicInteger counter = totalPendingByCluster.get(clusterName);
        return counter != null ? Math.max(0, counter.get()) : 0;
    }

}
