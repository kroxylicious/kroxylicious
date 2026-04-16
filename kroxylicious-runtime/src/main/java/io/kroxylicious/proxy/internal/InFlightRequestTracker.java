/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.Channel;

/**
 * Tracks pending Kafka request/response pairs per virtual cluster per channel.
 * <p>
 * A request is considered in-flight from the moment it is forwarded to the server
 * until the corresponding response is forwarded to the client. Thread-safe.
 */
public class InFlightRequestTracker {

    private final ConcurrentHashMap<String, ConcurrentHashMap<Channel, AtomicInteger>> pendingRequests = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> totalPendingByCluster = new ConcurrentHashMap<>();

    /**
     * Records that a request was forwarded to the server for the given cluster and channel.
     */
    public void onRequestSent(String clusterName, Channel channel) {
        pendingRequests
                .computeIfAbsent(clusterName, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(channel, k -> new AtomicInteger(0))
                .incrementAndGet();
        totalPendingByCluster
                .computeIfAbsent(clusterName, k -> new AtomicInteger(0))
                .incrementAndGet();
    }

    /**
     * Records that a response was received from the server for the given cluster and channel.
     */
    public void onResponseReceived(String clusterName, Channel channel) {
        ConcurrentHashMap<Channel, AtomicInteger> channelMap = pendingRequests.get(clusterName);
        if (channelMap != null) {
            AtomicInteger count = channelMap.get(channel);
            if (count != null) {
                int newValue = count.decrementAndGet();
                if (newValue < 0) {
                    count.set(0);
                }
            }
        }
        AtomicInteger total = totalPendingByCluster.get(clusterName);
        if (total != null) {
            int newValue = total.decrementAndGet();
            if (newValue < 0) {
                total.set(0);
            }
        }
    }

    /**
     * Cleans up tracking state for a closed channel.
     */
    public void onChannelClosed(String clusterName, Channel channel) {
        ConcurrentHashMap<Channel, AtomicInteger> channelMap = pendingRequests.get(clusterName);
        if (channelMap != null) {
            AtomicInteger removed = channelMap.remove(channel);
            if (removed != null && removed.get() > 0) {
                AtomicInteger total = totalPendingByCluster.get(clusterName);
                if (total != null) {
                    total.addAndGet(-removed.get());
                }
            }
        }
    }

    /**
     * Returns the number of pending requests for a specific channel.
     */
    public int getPendingRequestCount(String clusterName, Channel channel) {
        ConcurrentHashMap<Channel, AtomicInteger> channelMap = pendingRequests.get(clusterName);
        if (channelMap != null) {
            AtomicInteger count = channelMap.get(channel);
            if (count != null) {
                return Math.max(0, count.get());
            }
        }
        return 0;
    }

    /**
     * Returns the total number of pending requests across all channels for a cluster.
     */
    public int getTotalPendingRequestCount(String clusterName) {
        AtomicInteger total = totalPendingByCluster.get(clusterName);
        return total != null ? Math.max(0, total.get()) : 0;
    }
}
