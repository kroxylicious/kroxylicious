/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.channel.Channel;

/**
 * Tracks active downstream (client→proxy) and upstream (proxy→Kafka) channels per virtual cluster.
 * Thread-safe: uses ConcurrentHashMap with concurrent key sets.
 */
public class ConnectionTracker {

    private final ConcurrentHashMap<String, Set<Channel>> downstreamChannels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<Channel>> upstreamChannels = new ConcurrentHashMap<>();

    // ── Downstream (client → proxy) ──

    public void onDownstreamConnectionEstablished(String clusterName, Channel channel) {
        downstreamChannels.computeIfAbsent(clusterName, k -> ConcurrentHashMap.newKeySet()).add(channel);
    }

    public void onDownstreamConnectionClosed(String clusterName, Channel channel) {
        Set<Channel> channels = downstreamChannels.get(clusterName);
        if (channels != null) {
            channels.remove(channel);
        }
    }

    public Set<Channel> getDownstreamActiveChannels(String clusterName) {
        Set<Channel> channels = downstreamChannels.get(clusterName);
        return channels != null ? Collections.unmodifiableSet(channels) : Set.of();
    }

    // ── Upstream (proxy → Kafka) ──

    public void onUpstreamConnectionEstablished(String clusterName, Channel channel) {
        upstreamChannels.computeIfAbsent(clusterName, k -> ConcurrentHashMap.newKeySet()).add(channel);
    }

    public void onUpstreamConnectionClosed(String clusterName, Channel channel) {
        Set<Channel> channels = upstreamChannels.get(clusterName);
        if (channels != null) {
            channels.remove(channel);
        }
    }

    public Set<Channel> getUpstreamActiveChannels(String clusterName) {
        Set<Channel> channels = upstreamChannels.get(clusterName);
        return channels != null ? Collections.unmodifiableSet(channels) : Set.of();
    }

}
