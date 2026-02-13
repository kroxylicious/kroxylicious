/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

/**
 * Tracks active connections per virtual cluster to enable graceful shutdown and draining.
 * Supports both downstream (client-to-proxy) and upstream (proxy-to-broker) connections.
 * This class is thread-safe and can be used concurrently from multiple Netty event loops.
 */
public class ConnectionTracker {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionTracker.class);

    // Downstream connections (client → proxy)
    private final Map<String, AtomicInteger> downstreamConnections = new ConcurrentHashMap<>();
    private final Map<String, Set<Channel>> downstreamChannelsByCluster = new ConcurrentHashMap<>();

    // Upstream connections (proxy → target Kafka cluster)
    private final Map<String, AtomicInteger> upstreamConnections = new ConcurrentHashMap<>();
    private final Map<String, Set<Channel>> upstreamChannelsByCluster = new ConcurrentHashMap<>();

    /**
     * Records that a new downstream connection has been established for the given virtual cluster.
     *
     * @param clusterName the name of the virtual cluster
     * @param channel the channel representing the connection
     */
    public void onDownstreamConnectionEstablished(String clusterName, Channel channel) {
        downstreamConnections.computeIfAbsent(clusterName, k -> new AtomicInteger(0)).incrementAndGet();
        downstreamChannelsByCluster.computeIfAbsent(clusterName, k -> ConcurrentHashMap.newKeySet()).add(channel);

        LOGGER.debug("Downstream connection established for cluster '{}'. Downstream: {}, Upstream: {}",
                clusterName, getDownstreamActiveConnectionCount(clusterName), getUpstreamActiveConnectionCount(clusterName));
    }

    /**
     * Records that a downstream connection has been closed for the given virtual cluster.
     *
     * @param clusterName the name of the virtual cluster
     * @param channel the channel representing the connection
     */
    public void onDownstreamConnectionClosed(String clusterName, Channel channel) {
        onConnectionClosed(clusterName, channel, downstreamConnections, downstreamChannelsByCluster);

        LOGGER.debug("Downstream connection closed for cluster '{}'. Downstream: {}, Upstream: {}",
                clusterName, getDownstreamActiveConnectionCount(clusterName), getUpstreamActiveConnectionCount(clusterName));
    }

    /**
     * Gets the number of active downstream connections for a virtual cluster.
     *
     * @param clusterName the name of the virtual cluster
     * @return the number of active downstream connections
     */
    public int getDownstreamActiveConnectionCount(String clusterName) {
        AtomicInteger counter = downstreamConnections.get(clusterName);
        return counter != null ? Math.max(0, counter.get()) : 0;
    }

    /**
     * Gets all active downstream channels for a virtual cluster.
     *
     * @param clusterName the name of the virtual cluster
     * @return a set of active downstream channels
     */
    public Set<Channel> getDownstreamActiveChannels(String clusterName) {
        Set<Channel> channels = downstreamChannelsByCluster.get(clusterName);
        return channels != null ? Set.copyOf(channels) : Set.of();
    }

    // === UPSTREAM CONNECTION TRACKING ===

    /**
     * Records that a new upstream connection has been established for the given virtual cluster.
     *
     * @param clusterName the name of the virtual cluster
     * @param channel the channel representing the upstream connection
     */
    public void onUpstreamConnectionEstablished(String clusterName, Channel channel) {
        upstreamConnections.computeIfAbsent(clusterName, k -> new AtomicInteger(0)).incrementAndGet();
        upstreamChannelsByCluster.computeIfAbsent(clusterName, k -> ConcurrentHashMap.newKeySet()).add(channel);

        LOGGER.debug("Upstream connection established for cluster '{}'. Downstream: {}, Upstream: {}",
                clusterName, getDownstreamActiveConnectionCount(clusterName), getUpstreamActiveConnectionCount(clusterName));
    }

    /**
     * Records that an upstream connection has been closed for the given virtual cluster.
     *
     * @param clusterName the name of the virtual cluster
     * @param channel the channel representing the upstream connection
     */
    public void onUpstreamConnectionClosed(String clusterName, Channel channel) {
        onConnectionClosed(clusterName, channel, upstreamConnections, upstreamChannelsByCluster);

        LOGGER.debug("Upstream connection closed for cluster '{}'. Downstream: {}, Upstream: {}",
                clusterName, getDownstreamActiveConnectionCount(clusterName), getUpstreamActiveConnectionCount(clusterName));
    }

    /**
     * Gets the number of active upstream connections for a virtual cluster.
     *
     * @param clusterName the name of the virtual cluster
     * @return the number of active upstream connections
     */
    public int getUpstreamActiveConnectionCount(String clusterName) {
        AtomicInteger counter = upstreamConnections.get(clusterName);
        return counter != null ? Math.max(0, counter.get()) : 0;
    }

    /**
     * Gets all active upstream channels for a virtual cluster.
     *
     * @param clusterName the name of the virtual cluster
     * @return a set of active upstream channels
     */
    public Set<Channel> getUpstreamActiveChannels(String clusterName) {
        Set<Channel> channels = upstreamChannelsByCluster.get(clusterName);
        return channels != null ? Set.copyOf(channels) : Set.of();
    }

    /**
     * Gets the total number of connections (downstream + upstream) for a virtual cluster.
     *
     * @param clusterName the name of the virtual cluster
     * @return the total number of connections
     */
    public int getTotalConnectionCount(String clusterName) {
        return getDownstreamActiveConnectionCount(clusterName) + getUpstreamActiveConnectionCount(clusterName);
    }

    /**
     * Common method to remove a connection and clean up empty entries.
     * This method decrements the connection counter and removes the channel from the set,
     * cleaning up empty entries to prevent memory leaks.
     *
     * @param clusterName the name of the virtual cluster
     * @param channel the channel to remove
     * @param connectionCounters the map of connection counters
     * @param channelsByCluster the map of channels by cluster
     */
    private void onConnectionClosed(String clusterName, Channel channel,
                                    Map<String, AtomicInteger> connectionCounters,
                                    Map<String, Set<Channel>> channelsByCluster) {
        // Decrement counter and remove if zero or negative
        AtomicInteger counter = connectionCounters.get(clusterName);
        if (counter != null) {
            counter.decrementAndGet();
            if (counter.get() <= 0) {
                connectionCounters.remove(clusterName);
            }
        }

        // Remove channel from set and remove empty sets
        Set<Channel> channels = channelsByCluster.get(clusterName);
        if (channels != null) {
            channels.remove(channel);
            if (channels.isEmpty()) {
                channelsByCluster.remove(clusterName);
            }
        }
    }

}