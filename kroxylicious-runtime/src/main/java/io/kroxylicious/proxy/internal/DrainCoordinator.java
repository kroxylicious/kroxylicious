/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * Lightweight registry that bridges proxy-level drain requests to per-connection
 * {@link ProxyChannelStateMachine} instances.
 * <p>
 * Each PCSM registers on {@code toClientActive()} and deregisters on {@code toClosed()}.
 * {@link #drainCluster(String, Duration)} asks every registered PCSM to close itself by
 * invoking {@link ProxyChannelStateMachine#initiateClose(Duration)}, which returns a future
 * that completes when that connection has reached {@code Closed} (either naturally or after
 * the timeout force-closes it). The coordinator only aggregates those futures — the per-
 * connection orchestration (timer, policy assembly, event-loop dispatch) lives inside the
 * PCSM.
 * <p>
 * Works for both proxy shutdown (drain all clusters) and hot-reload (drain one cluster
 * while others keep serving).
 *
 * <h2>Thread safety</h2>
 * Instances of this class are safe for concurrent use by multiple threads.  Specifically:
 * <ul>
 *   <li>{@link #register(String, ProxyChannelStateMachine)} and
 *       {@link #deregister(String, ProxyChannelStateMachine)} are atomic per cluster: their
 *       read-modify-write of the cluster's PCSM set is guarded by
 *       {@link java.util.concurrent.ConcurrentHashMap#compute compute}/{@code computeIfPresent},
 *       which lock the bucket for the lambda's duration. A {@code register} on cluster {@code A}
 *       does not block a concurrent {@code register} on cluster {@code B}.</li>
 *   <li>{@link #drainCluster(String, Duration)} reads the cluster's PCSM set via
 *       {@link java.util.concurrent.ConcurrentHashMap#getOrDefault getOrDefault} and iterates a
 *       defensive copy. Concurrent {@code register}/{@code deregister} during the iteration
 *       does not corrupt the snapshot.</li>
 * </ul>
 */
public class DrainCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DrainCoordinator.class);

    private final ConcurrentHashMap<String, Set<ProxyChannelStateMachine>> activeConnections = new ConcurrentHashMap<>();

    public void register(String clusterName, ProxyChannelStateMachine pcsm) {
        activeConnections.compute(clusterName, (key, existingSet) -> {
            Set<ProxyChannelStateMachine> set = (existingSet != null) ? existingSet : ConcurrentHashMap.newKeySet();
            set.add(pcsm);
            return set;
        });
    }

    public void deregister(String clusterName, ProxyChannelStateMachine pcsm) {
        activeConnections.computeIfPresent(clusterName, (key, set) -> {
            set.remove(pcsm);
            return set.isEmpty() ? null : set;
        });
    }

    /**
     * Drains all connections for the given cluster. Returns a future that completes
     * when all connections have reached the Closed state (or the timeout expires).
     *
     * @param clusterName the virtual cluster to drain
     * @param timeout maximum time to wait for in-flight requests per connection
     * @return future completing when all connections for this cluster are closed
     */
    public CompletableFuture<Void> drainCluster(String clusterName, Duration timeout) {
        Set<ProxyChannelStateMachine> connections = activeConnections.getOrDefault(clusterName, Set.of());
        if (connections.isEmpty()) {
            LOGGER.atInfo()
                    .addKeyValue("virtualCluster", clusterName)
                    .log("No active connections to drain");
            return CompletableFuture.completedFuture(null);
        }

        LOGGER.atInfo()
                .addKeyValue("virtualCluster", clusterName)
                .addKeyValue("connectionCount", connections.size())
                .addKeyValue("timeoutMs", timeout.toMillis())
                .log("Starting cluster drain");

        var closedFutures = new ArrayList<CompletableFuture<Void>>();

        // Defensive copy: the concurrent set is modified as PCSMs deregister during toClosed().
        // The per-connection drain orchestration (timer scheduling, policy assembly, event-loop
        // dispatch) lives inside the PCSM itself — see ProxyChannelStateMachine#initiateClose.
        for (ProxyChannelStateMachine pcsm : new ArrayList<>(connections)) {
            closedFutures.add(pcsm.initiateClose(timeout));
        }

        return CompletableFuture.allOf(closedFutures.toArray(CompletableFuture[]::new));
    }

    /**
     * Returns an immutable snapshot of the PCSMs currently registered for the named cluster.
     * Empty if the cluster has no active connections.
     */
    @VisibleForTesting
    Set<ProxyChannelStateMachine> activeConnectionsFor(String clusterName) {
        Set<ProxyChannelStateMachine> set = activeConnections.get(clusterName);
        return set == null ? Set.of() : Set.copyOf(set);
    }
}
