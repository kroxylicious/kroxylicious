/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
public class VirtualClusterRegistry {

    private final List<VirtualClusterModel> virtualClusterModels;
    private final Map<String, VirtualClusterLifecycle> lifecyclesByCluster;
    private final BiConsumer<String, Optional<Throwable>> onVirtualClusterStopped;

    /**
     * Creates a new VirtualClusterRegistry for the given set of virtual clusters.
     *
     * @param virtualClusterModels the complete set of virtual cluster configurations
     * @param onVirtualClusterStopped callback invoked with {@code (clusterName, priorFailureCause)}
     *        whenever a virtual cluster reaches the terminal Stopped state. The cause is empty
     *        for clean stops (e.g. drain completed during shutdown) and present for failure-driven stops.
     *        The callback must not throw exceptions.
     * @throws NullPointerException if either argument is null
     * @throws IllegalArgumentException if the list contains duplicate cluster names
     */
    public VirtualClusterRegistry(List<VirtualClusterModel> virtualClusterModels,
                                  BiConsumer<String, Optional<Throwable>> onVirtualClusterStopped) {
        Objects.requireNonNull(virtualClusterModels, "virtualClusterModels must not be null");
        this.onVirtualClusterStopped = Objects.requireNonNull(onVirtualClusterStopped, "onVirtualClusterStopped must not be null");
        this.virtualClusterModels = List.copyOf(virtualClusterModels);
        this.lifecyclesByCluster = new LinkedHashMap<>();
        for (var vcm : this.virtualClusterModels) {
            var name = vcm.getClusterName();
            if (lifecyclesByCluster.containsKey(name)) {
                throw new IllegalArgumentException("Duplicate cluster name: " + name);
            }
            lifecyclesByCluster.put(name, new VirtualClusterLifecycle(name, vcm.drainTimeout()));
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
        var lifecycle = requireKnownCluster(clusterName);
        lifecycle.initializationFailed(cause);
        lifecycle.stop();
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
    public void shutdownAllClusters() {
        var drainFutures = new ArrayList<CompletableFuture<Void>>();
        lifecyclesByCluster.forEach((name, lifecycle) -> {
            var state = lifecycle.state();
            if (state instanceof VirtualClusterLifecycleState.Serving) {
                drainFutures.add(lifecycle.startDraining()
                        .thenRun(() -> {
                            lifecycle.drainComplete();
                            onVirtualClusterStopped.accept(name, Optional.empty());
                        }));
            }
            else if (state instanceof VirtualClusterLifecycleState.Draining) {
                // Pre-existing drain (e.g. hot-reload) — join it rather than starting a new one.
                drainFutures.add(lifecycle.drainFuture()
                        .thenRun(() -> {
                            lifecycle.drainComplete();
                            onVirtualClusterStopped.accept(name, Optional.empty());
                        }));
            }
            else if (state instanceof VirtualClusterLifecycleState.Failed failed) {
                lifecycle.stop();
                onVirtualClusterStopped.accept(name, Optional.of(failed.cause()));
            }
            else if (state instanceof VirtualClusterLifecycleState.Stopped) {
                // it's already dead, let sleeping dogs lie
            }
            else {
                lifecycle.stop();
                onVirtualClusterStopped.accept(name, Optional.empty());
            }
        });
        CompletableFuture.allOf(drainFutures.toArray(CompletableFuture[]::new)).join();
    }

    /**
     * Returns the lifecycle for the given virtual cluster name.
     * @param clusterName the virtual cluster name
     * @return the lifecycle, or null if no cluster with that name exists
     */
    @Nullable
    public VirtualClusterLifecycle lifecycleFor(String clusterName) {
        return lifecyclesByCluster.get(clusterName);
    }

    public boolean registerConnection(String clusterName, ProxyChannelStateMachine pcsm) {
        return requireKnownCluster(clusterName).registerConnection(pcsm);
    }

    public void deregisterConnection(String clusterName, ProxyChannelStateMachine pcsm) {
        requireKnownCluster(clusterName).deregisterConnection(pcsm);
    }

    @VisibleForTesting
    Set<ProxyChannelStateMachine> activeConnectionsFor(String clusterName) {
        return requireKnownCluster(clusterName).activeConnections();
    }

    private VirtualClusterLifecycle requireKnownCluster(String clusterName) {
        var lifecycle = lifecyclesByCluster.get(clusterName);
        if (lifecycle == null) {
            throw new IllegalArgumentException("Unknown cluster: " + clusterName);
        }
        return lifecycle;
    }
}
