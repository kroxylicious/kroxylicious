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
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterRegistry.class);

    /**
     * Pairs each tracked virtual cluster's static {@link VirtualClusterModel} with its mutable
     * {@link VirtualClusterLifecycle}.
     */
    private record VirtualClusterEntry(VirtualClusterModel model, VirtualClusterLifecycle lifecycle) {}

    private final Map<String, VirtualClusterEntry> entriesByCluster;
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
        this.entriesByCluster = new LinkedHashMap<>();
        for (var vcm : virtualClusterModels) {
            var name = vcm.getClusterName();
            if (entriesByCluster.containsKey(name)) {
                throw new IllegalArgumentException("Duplicate cluster name: " + name);
            }
            entriesByCluster.put(name, new VirtualClusterEntry(vcm, new VirtualClusterLifecycle(name, vcm.drainTimeout())));
        }
    }

    /**
     * Returns the currently-tracked virtual cluster models. The list reflects the constructor-
     * supplied models PLUS any added at runtime via {@link #addVirtualCluster(VirtualClusterModel)}.
     *
     * @return snapshot of currently-tracked virtual cluster models (insertion order)
     */
    public List<VirtualClusterModel> virtualClusterModels() {
        return entriesByCluster.values().stream()
                .map(VirtualClusterEntry::model)
                .toList();
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
        var drainFutures = entriesByCluster.entrySet().stream()
                .map(e -> shutdownCluster(e.getKey(), e.getValue().lifecycle()))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(drainFutures).join();
    }

    /**
     * Drives a single virtual cluster to its terminal {@code Stopped} state, regardless of
     * which non-terminal state it is currently in. Shared by {@link #shutdownAllClusters()}
     * (which drives every cluster) and {@link #removeVirtualCluster(String)} (which drives one).
     *
     * <p>State handling matches the proxy-wide shutdown semantics:
     * <ul>
     *   <li>{@code Serving} → start draining, then transition to {@code Stopped} once
     *       connections have drained; fires {@link #onVirtualClusterStopped} with empty cause</li>
     *   <li>{@code Draining} (e.g. concurrent shutdown / reconfigure) → join the existing
     *       drain future, then transition to {@code Stopped}; fires callback with empty cause</li>
     *   <li>{@code Failed} → transition to {@code Stopped} synchronously; fires callback with
     *       the prior failure cause</li>
     *   <li>{@code Stopped} → no-op (cluster is already terminal)</li>
     *   <li>{@code Initializing} → transition to {@code Stopped} synchronously; fires callback
     *       with empty cause</li>
     * </ul>
     *
     * <h2>Entries are retained in {@link #entriesByCluster} after reaching {@code Stopped}.</h2>
     * The map is append-only — driving a cluster to {@code Stopped} never deletes its entry.
     * No caller currently depends on this retention for correctness:
     * {@link #registerConnection(String, ClientConnectionStateMachine)} and
     * {@link #deregisterConnection(String, ClientConnectionStateMachine)} both tolerate a
     * missing entry, and {@code RemoveCluster} receives its model from the planner at plan
     * time rather than re-reading the registry after the lifecycle reaches {@code Stopped}.
     *
     * @return a future that completes when the cluster has reached {@code Stopped}
     */
    private CompletableFuture<Void> shutdownCluster(String clusterName, VirtualClusterLifecycle lifecycle) {
        var state = lifecycle.state();
        if (state instanceof VirtualClusterLifecycleState.Serving) {
            return lifecycle.startDraining()
                    .thenRun(() -> {
                        lifecycle.drainComplete();
                        onVirtualClusterStopped.accept(clusterName, Optional.empty());
                    });
        }
        else if (state instanceof VirtualClusterLifecycleState.Draining) {
            // Pre-existing drain (e.g. concurrent shutdown or hot-reload) — join it rather
            // than starting a new one.
            return lifecycle.drainFuture()
                    .thenRun(() -> {
                        lifecycle.drainComplete();
                        onVirtualClusterStopped.accept(clusterName, Optional.empty());
                    });
        }
        else if (state instanceof VirtualClusterLifecycleState.Failed failed) {
            lifecycle.stop();
            onVirtualClusterStopped.accept(clusterName, Optional.of(failed.cause()));
            return CompletableFuture.completedFuture(null);
        }
        else if (state instanceof VirtualClusterLifecycleState.Stopped) {
            // Already dead, let sleeping dogs lie.
            return CompletableFuture.completedFuture(null);
        }
        else {
            // Initializing — transition to Stopped via the dedicated stop() method.
            lifecycle.stop();
            onVirtualClusterStopped.accept(clusterName, Optional.empty());
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Returns the lifecycle for the given virtual cluster name.
     * @param clusterName the virtual cluster name
     * @return the lifecycle, or null if no cluster with that name exists
     */
    @Nullable
    public VirtualClusterLifecycle lifecycleFor(String clusterName) {
        var entry = entriesByCluster.get(clusterName);
        return entry == null ? null : entry.lifecycle();
    }

    /**
     * Attempts to register a new connection for {@code clusterName}.
     *
     * @return {@code true} iff the cluster is known to this registry AND its lifecycle is in a
     *         state that accepts new connections (i.e. {@code SERVING}). An unknown cluster is
     *         treated as a rejection rather than an error so that {@code KafkaProxyInitializer}'s
     *         existing {@code false → rejectConnection} path covers both "not serving" and "no
     *         such cluster" without depending on the bookkeeping-vs-binding ordering invariant
     *         being preserved by future changes.
     */
    public boolean registerConnection(String clusterName, ClientConnectionStateMachine ccsm) {
        var entry = entriesByCluster.get(clusterName);
        if (entry == null) {
            // Unreachable under the current bookkeeping-before-binding ordering and append-only
            // entry policy — log loudly so the broken invariant doesn't hide behind the clean
            // false-return path.
            LOGGER.atWarn()
                    .addKeyValue("virtualCluster", clusterName)
                    .log("registerConnection called for unknown virtual cluster; rejecting connection");
            return false;
        }
        return entry.lifecycle().registerConnection(ccsm);
    }

    /**
     * Decrements the active-connections count for {@code clusterName} if
     * the cluster is no longer known to this registry. Called from a Netty channel-close
     * listener, which can race against entry removal in a future cleanup-on-{@code Stopped}
     */
    public void deregisterConnection(String clusterName, ClientConnectionStateMachine ccsm) {
        var entry = entriesByCluster.get(clusterName);
        if (entry == null) {
            // Unreachable under the current append-only entry policy — an entry that was present
            // at registerConnection time must still be present at channel-close time. Logged so
            // a future cleanup-on-Stopped change that violates this invariant is observable.
            LOGGER.atWarn()
                    .addKeyValue("virtualCluster", clusterName)
                    .log("deregisterConnection called for unknown virtual cluster; ignoring");
            return;
        }
        entry.lifecycle().deregisterConnection(ccsm);
    }

    @VisibleForTesting
    Set<ClientConnectionStateMachine> activeConnectionsFor(String clusterName) {
        return requireKnownCluster(clusterName).activeConnections();
    }

    /**
     * Drives an existing virtual cluster through {@code SERVING → DRAINING → STOPPED}.
     * Invoked by {@code ConfigurationReloadOrchestrator} for clusters present in the running
     * configuration but absent in the submitted one.
     *
     * <p>The cluster's entry is <strong>not</strong> removed from the registry on reaching
     * {@code Stopped} — see {@link #shutdownCluster(String, VirtualClusterLifecycle)} for the
     * rationale.
     *
     * @param clusterName the virtual cluster to remove; must name an existing cluster
     * @return a future that completes when the cluster has reached {@code Stopped}
     * @throws IllegalArgumentException if {@code clusterName} does not name a registered cluster
     */
    public CompletableFuture<Void> removeVirtualCluster(String clusterName) {
        var lifecycle = requireKnownCluster(clusterName);
        LOGGER.atInfo()
                .addKeyValue("virtualCluster", clusterName)
                .addKeyValue("operation", "removeVirtualCluster")
                .log("reconfigure: removing virtual cluster");
        return shutdownCluster(clusterName, lifecycle);
    }

    /**
     * Drives an existing virtual cluster through {@code SERVING → DRAINING → INITIALIZING → SERVING}
     * with the supplied new model. Invoked by {@code ConfigurationReloadOrchestrator} for
     * clusters whose configuration differs between the running and submitted configurations.
     *
     * <p>Named by its <em>intent</em> (apply {@code newModel} to the cluster identified by
     * {@code clusterName}) rather than its implementation; a future iteration may implement
     * replace more surgically (filter-chain swap on existing connections, rolling handoff)
     * without changing the caller's interface.
     *
     * @param clusterName the virtual cluster to replace; must name an existing cluster
     * @param newModel    the new model to apply
     * @return a future that completes when the replacement is finished
     */
    public CompletableFuture<Void> replaceVirtualCluster(String clusterName, VirtualClusterModel newModel) {
        // TODO: implement SERVING -> DRAINING -> [drain] -> [deregister] -> INITIALIZING ->
        // [register] -> SERVING in the follow-up PR. See removeVirtualCluster Javadoc.
        LOGGER.atWarn()
                .addKeyValue("virtualCluster", clusterName)
                .addKeyValue("operation", "replaceVirtualCluster")
                .log("reconfigure: per-VC lifecycle transitions not yet implemented; no-op stub invoked");
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Creates a {@link VirtualClusterLifecycle} in {@code INITIALIZING} for
     * the given model. Endpoint binding and the transition to {@code SERVING} are the
     * orchestrator's responsibility — once gateway registration succeeds it calls
     * {@link #initializationSucceeded(String)}; on failure it calls
     * {@link #initializationFailed(String, Throwable)} and rolls back the gateway bindings.
     *
     * @param newModel the model for the new cluster; must not name a cluster already present
     * @return an already-completed future (the operation is synchronous; the
     *         {@link CompletableFuture} shape is preserved for caller symmetry with
     *         {@link #removeVirtualCluster})
     * @throws IllegalArgumentException if a cluster with this name already exists
     */
    public CompletableFuture<Void> addVirtualCluster(VirtualClusterModel newModel) {
        Objects.requireNonNull(newModel, "newModel must not be null");
        String name = newModel.getClusterName();
        if (entriesByCluster.containsKey(name)) {
            throw new IllegalArgumentException("Cluster already exists: " + name);
        }
        LOGGER.atInfo()
                .addKeyValue("virtualCluster", name)
                .addKeyValue("operation", "addVirtualCluster")
                .log("reconfigure: created lifecycle in INITIALIZING; gateway registration is the orchestrator's responsibility");
        entriesByCluster.put(name, new VirtualClusterEntry(newModel, new VirtualClusterLifecycle(name, newModel.drainTimeout())));
        return CompletableFuture.completedFuture(null);
    }

    private VirtualClusterLifecycle requireKnownCluster(String clusterName) {
        var entry = entriesByCluster.get(clusterName);
        if (entry == null) {
            throw new IllegalArgumentException("Unknown cluster: " + clusterName);
        }
        return entry.lifecycle();
    }
}
