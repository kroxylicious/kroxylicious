/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Owns the virtual cluster configuration tree and lifecycle state.
 * <p>
 * This is the single source of truth for which virtual clusters exist and their
 * current lifecycle state. It does not manage networking, endpoint registration,
 * metrics, or any Netty infrastructure — those remain with {@link io.kroxylicious.proxy.KafkaProxy}.
 * </p>
 * <p>
 * The {@code onVirtualClusterStopped} callback notifies the owner (typically KafkaProxy)
 * when a virtual cluster reaches the terminal {@link VirtualClusterLifecycleState.Stopped}
 * state, allowing the owner to apply proxy-level policy (e.g. {@code serve: none}).
 * During reload, the Draining → Initializing → Serving cycle is managed internally
 * without involving the callback — reload never reaches Stopped.
 * </p>
 * <p>
 * Each virtual cluster's per-cluster state machine is a {@link VirtualClusterLifecycle}.
 * </p>
 */
public class VirtualClusterCoordinator {

    private final List<VirtualClusterModel> virtualClusterModels;
    private final Map<String, VirtualClusterLifecycle> lifecycleManagers;
    private final BiConsumer<String, Optional<Throwable>> onVirtualClusterStopped;

    /**
     * Creates a new VirtualClusterCoordinator for the given set of virtual clusters.
     *
     * @param virtualClusterModels the complete set of virtual cluster configurations
     * @param onVirtualClusterStopped callback invoked with {@code (clusterName, priorFailureCause)}
     *        whenever a virtual cluster reaches the terminal Stopped state. The cause is empty
     *        for clean stops (e.g. drain completed during shutdown) and present for failure-driven stops.
     *        The callback must not throw exceptions.
     * @throws NullPointerException if either argument is null
     * @throws IllegalArgumentException if the list contains duplicate cluster names
     */
    public VirtualClusterCoordinator(List<VirtualClusterModel> virtualClusterModels,
                                     BiConsumer<String, Optional<Throwable>> onVirtualClusterStopped) {
        Objects.requireNonNull(virtualClusterModels, "virtualClusterModels must not be null");
        this.onVirtualClusterStopped = Objects.requireNonNull(onVirtualClusterStopped, "onVirtualClusterStopped must not be null");
        this.virtualClusterModels = List.copyOf(virtualClusterModels);
        this.lifecycleManagers = new LinkedHashMap<>();
        for (var vcm : this.virtualClusterModels) {
            var name = vcm.getClusterName();
            if (lifecycleManagers.containsKey(name)) {
                throw new IllegalArgumentException("Duplicate cluster name: " + name);
            }
            lifecycleManagers.put(name, new VirtualClusterLifecycle(name, vcm.drainTimeout()));
        }
    }

    /**
     * Returns the virtual cluster models this manager was constructed with.
     * @return unmodifiable list of virtual cluster models
     */
    public List<VirtualClusterModel> virtualClusterModels() {
        return virtualClusterModels;
    }

    /**
     * Signals that the named virtual cluster initialized successfully.
     * Transitions the cluster from Initializing to Serving.
     *
     * @param clusterName the virtual cluster name
     * @throws IllegalArgumentException if no cluster with that name exists
     */
    public void initializationSucceeded(String clusterName) {
        requireKnownCluster(clusterName).initializationSucceeded();
    }

    /**
     * Signals that the named virtual cluster failed to initialize.
     * Transitions the cluster from Initializing to Failed, then immediately to Stopped
     * (no recovery path exists today), and fires the {@code onVirtualClusterStopped} callback.
     *
     * @param clusterName the virtual cluster name
     * @param cause the failure cause
     * @throws IllegalArgumentException if no cluster with that name exists
     */
    public void initializationFailed(String clusterName, Throwable cause) {
        var manager = requireKnownCluster(clusterName);
        manager.initializationFailed(cause);
        manager.stop();
        onVirtualClusterStopped.accept(clusterName, Optional.of(cause));
    }

    /**
     * Transitions all virtual clusters toward draining/stopped as appropriate for shutdown.
     * <ul>
     *   <li>Serving → Draining</li>
     *   <li>Draining → Draining (a pre-existing drain, e.g. from hot-reload, is left to complete)</li>
     *   <li>Initializing → Stopped (fires callback with empty cause)</li>
     *   <li>Failed → Stopped (fires callback with cause)</li>
     *   <li>Stopped → Stopped (no-op)</li>
     * </ul>
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public void initiateShutdown() {
        lifecycleManagers.forEach((name, manager) -> {
            var state = manager.state();
            if (state instanceof VirtualClusterLifecycleState.Serving) {
                manager.startDraining();
            }
            else if (state instanceof VirtualClusterLifecycleState.Failed failed) {
                manager.stop();
                onVirtualClusterStopped.accept(name, Optional.of(failed.cause()));
            }
            else if (state instanceof VirtualClusterLifecycleState.Draining) {
                // we leave draining clusters in Draining.
            }
            else if (state instanceof VirtualClusterLifecycleState.Stopped) {
                // its already dead, let sleeping dogs lie
            }
            else {
                manager.stop();
                onVirtualClusterStopped.accept(name, Optional.empty());
            }
        });
    }

    /**
     * Completes the shutdown by transitioning all draining virtual clusters to stopped,
     * firing the callback for each.
     *
     * @return true if all virtual clusters are now in the Stopped state
     */
    public boolean completeDraining() {
        lifecycleManagers.forEach((name, manager) -> {
            var state = manager.state();
            if (state instanceof VirtualClusterLifecycleState.Draining) {
                manager.drainComplete();
                onVirtualClusterStopped.accept(name, Optional.empty());
            }
        });
        return lifecycleManagers.values().stream()
                .allMatch(m -> m.state() instanceof VirtualClusterLifecycleState.Stopped);
    }

    /**
     * Returns the lifecycle for the given virtual cluster name.
     * @param clusterName the virtual cluster name
     * @return the lifecycle, or null if no cluster with that name exists
     */
    @Nullable
    public VirtualClusterLifecycle lifecycleFor(String clusterName) {
        return lifecycleManagers.get(clusterName);
    }

    public void registerConnection(String clusterName, ProxyChannelStateMachine pcsm) {
        requireKnownCluster(clusterName).registerConnection(pcsm);
    }

    public void deregisterConnection(String clusterName, ProxyChannelStateMachine pcsm) {
        requireKnownCluster(clusterName).deregisterConnection(pcsm);
    }

    @VisibleForTesting
    Set<ProxyChannelStateMachine> activeConnectionsFor(String clusterName) {
        var lifecycle = lifecycleManagers.get(clusterName);
        return lifecycle == null ? Set.of() : lifecycle.activeConnections();
    }

    private VirtualClusterLifecycle requireKnownCluster(String clusterName) {
        var manager = lifecycleManagers.get(clusterName);
        if (manager == null) {
            throw new IllegalArgumentException("Unknown cluster: " + clusterName);
        }
        return manager;
    }
}
