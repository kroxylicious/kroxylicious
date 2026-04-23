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
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight registry that bridges proxy-level drain requests to per-connection
 * {@link ProxyChannelStateMachine} instances.
 * <p>
 * Each PCSM registers on {@code toClientActive()} and deregisters on {@code toClosed()}.
 * {@link #drainCluster(String, Duration)} dispatches {@code onDraining(Runnable)} to each
 * PCSM's event loop, injecting a policy that completes the per-connection future, cancels
 * the timeout timer, and asks the PCSM to close with the {@code DRAIN_COMPLETED} cause.
 * <p>
 * The coordinator also owns the per-connection timeout timer. When it expires, the
 * coordinator fires {@link ProxyChannelStateMachine#onDrainTimeout()} so the PCSM records
 * the {@code DRAIN_TIMEOUT} cause before closing.
 * <p>
 * Works for both proxy shutdown (drain all clusters) and hot-reload (drain one cluster
 * while others keep serving).
 */
public class DrainCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DrainCoordinator.class);

    private final ConcurrentHashMap<String, Set<ProxyChannelStateMachine>> activeConnections = new ConcurrentHashMap<>();

    public void register(String clusterName, ProxyChannelStateMachine pcsm) {
        activeConnections.computeIfAbsent(clusterName, k -> ConcurrentHashMap.newKeySet()).add(pcsm);
    }

    public void deregister(String clusterName, ProxyChannelStateMachine pcsm) {
        Set<ProxyChannelStateMachine> set = activeConnections.get(clusterName);
        if (set != null) {
            set.remove(pcsm);
            if (set.isEmpty()) {
                activeConnections.remove(clusterName);
            }
        }
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

        // Defensive copy: the concurrent set is modified as PCSMs deregister during toClosed()
        for (ProxyChannelStateMachine pcsm : new ArrayList<>(connections)) {
            CompletableFuture<Void> closedFuture = new CompletableFuture<>();
            closedFutures.add(closedFuture);

            // Schedule the force-close timer. Fires pcsm.onDrainTimeout() on the event loop,
            // which transitions the PCSM to Closed with DRAIN_TIMEOUT if it's still Draining.
            ScheduledFuture<?> timeoutTask = pcsm.scheduleOnEventLoop(pcsm::onDrainTimeout, timeout);

            // Policy injected into the PCSM's Draining state. Fires when the PCSM evaluates
            // "counter hit zero" OR when toClosed transitions out of Draining for any reason.
            // Idempotent: cancel and complete are no-ops when already invoked; onDrainCompleted
            // no-ops when state is already Closed.
            Runnable onDrained = () -> {
                timeoutTask.cancel(false);
                closedFuture.complete(null);
                pcsm.onDrainCompleted();
            };

            pcsm.executeOnEventLoop(() -> pcsm.onDraining(onDrained));
        }

        return CompletableFuture.allOf(closedFutures.toArray(CompletableFuture[]::new));
    }
}
