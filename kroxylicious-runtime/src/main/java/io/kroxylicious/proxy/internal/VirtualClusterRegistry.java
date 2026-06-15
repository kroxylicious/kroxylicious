/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.Configuration;
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
public class VirtualClusterRegistry implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterRegistry.class);

    /**
     * Pairs each tracked virtual cluster's static {@link VirtualClusterModel} with its mutable
     * {@link VirtualClusterLifecycle}.
     */
    private record VirtualClusterEntry(VirtualClusterModel model, VirtualClusterLifecycle lifecycle) {}

    private final Map<String, VirtualClusterEntry> entriesByCluster = new ConcurrentHashMap<>();
    private final BiConsumer<String, Optional<Throwable>> onVirtualClusterStopped;
    private final BiFunction<Configuration, String, VirtualClusterModel> rawModelResolver;

    /**
     * Thread name prefix for the lifecycle executor.
     */
    static final String LIFECYCLE_THREAD_NAME_PREFIX = "kroxylicious-vc-lifecycle";

    private static final AtomicInteger LIFECYCLE_THREAD_COUNTER = new AtomicInteger();

    /**
     * Dedicated single-threaded executor that owns every {@code FilterFactory.initialize()}
     * invocation triggered through this registry (via {@link #resolveModel}) and the close work
     * for the Serving/Draining branches of {@link #shutdownCluster}. Created here so the
     * threading guarantee is a VCR-internal invariant: nothing outside VCR can submit work to
     * this executor. Shut down by {@link #close()}.
     */
    private final ExecutorService lifecycleExecutor = Executors.newSingleThreadExecutor(r -> {
        var t = new Thread(r, LIFECYCLE_THREAD_NAME_PREFIX + "-" + LIFECYCLE_THREAD_COUNTER.incrementAndGet());
        t.setDaemon(true);
        return t;
    });

    /**
     * Creates a new VirtualClusterRegistry for the given set of virtual clusters.
     *
     * @param virtualClusterModels the complete set of virtual cluster configurations
     * @param rawModelResolver builds a {@link VirtualClusterModel} for a (config, clusterName)
     *        pair without any threading concerns. The registry wraps every invocation in a
     *        dispatch to the lifecycle executor — see {@link #resolveModel}. Captured at
     *        construction time so plugin-factory wiring is the responsibility of the registry's
     *        owner (typically KafkaProxy), not the registry itself.
     * @param onVirtualClusterStopped callback invoked with {@code (clusterName, priorFailureCause)}
     *        whenever a virtual cluster reaches the terminal Stopped state. The cause is empty
     *        for clean stops (e.g. drain completed during shutdown) and present for failure-driven stops.
     *        The callback must not throw exceptions.
     * @throws NullPointerException if any argument is null
     * @throws IllegalArgumentException if the list contains duplicate cluster names
     */
    public VirtualClusterRegistry(List<VirtualClusterModel> virtualClusterModels,
                                  BiFunction<Configuration, String, VirtualClusterModel> rawModelResolver,
                                  BiConsumer<String, Optional<Throwable>> onVirtualClusterStopped) {
        Objects.requireNonNull(virtualClusterModels, "virtualClusterModels must not be null");
        this.rawModelResolver = Objects.requireNonNull(rawModelResolver, "rawModelResolver must not be null");
        this.onVirtualClusterStopped = Objects.requireNonNull(onVirtualClusterStopped, "onVirtualClusterStopped must not be null");
        for (var vcm : virtualClusterModels) {
            var name = vcm.getClusterName();
            if (entriesByCluster.containsKey(name)) {
                throw new IllegalArgumentException("Duplicate cluster name: " + name);
            }
            entriesByCluster.put(name, new VirtualClusterEntry(vcm, new VirtualClusterLifecycle(name, vcm.drainTimeout())));
        }
    }

    /**
     * Builds a {@link VirtualClusterModel} for the named virtual cluster, dispatched onto the
     * lifecycle thread. Used by {@code OperationsPlanner} during reconfigure so each filter's
     * {@code initialize()} runs on a non-event-loop thread regardless of which thread invoked
     * {@code reconfigure()}.
     *
     * @throws RuntimeException the same RuntimeException the underlying resolver threw, or a
     *         {@link CompletionException} wrapping a checked exception
     */
    public VirtualClusterModel resolveModel(Configuration config, String clusterName) {
        return runOnLifecycle(() -> rawModelResolver.apply(config, clusterName));
    }

    /**
     * Submits a task to the lifecycle executor and waits for its result. Errors thrown by the
     * task are caught (not just Exception) so they're forwarded to the caller via the future
     * rather than killing the single lifecycle thread.
     */
    @SuppressWarnings("java:S1181")
    private <T> T runOnLifecycle(Callable<T> task) {
        var future = new CompletableFuture<T>();
        lifecycleExecutor.execute(() -> {
            try {
                future.complete(task.call());
            }
            catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        try {
            return future.join();
        }
        catch (CompletionException e) {
            var cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new CompletionException(cause);
        }
    }

    /**
     * Shuts down the lifecycle executor. Must be called after {@link #shutdownAllClusters()}
     * has drained — that method dispatches close work through the executor, so tearing the
     * executor down first would reject straggling submissions.
     */
    @Override
    public void close() {
        lifecycleExecutor.shutdown();
        try {
            if (!lifecycleExecutor.awaitTermination(15, TimeUnit.SECONDS)) {
                LOGGER.atWarn().log("Lifecycle executor did not terminate within 15s; forcing shutdown");
                lifecycleExecutor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            lifecycleExecutor.shutdownNow();
        }
    }

    /**
     * Returns the currently-tracked virtual cluster models. The collection reflects the constructor-
     * supplied models PLUS any added at runtime via {@link #addVirtualCluster(VirtualClusterModel)}.
     *
     * <p>Iteration order is unspecified (the backing map is concurrent). Callers that need
     * order-stable output should sort the result themselves.
     *
     * @return weakly-consistent snapshot of currently-tracked virtual cluster models
     */
    public Collection<VirtualClusterModel> virtualClusterModels() {
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
     *   <li>{@code Serving} → begin draining; when drain completes, re-check state, transition
     *       to {@code Stopped}, and close on the lifecycle thread</li>
     *   <li>{@code Draining} → join the existing drain; same close-with-re-check tail as Serving</li>
     *   <li>{@code Failed} → dispatch a re-check-and-close task to the lifecycle thread; on the
     *       lifecycle thread, if state is still {@code Failed}, transition and close with the
     *       prior failure cause</li>
     *   <li>{@code Initializing} → same as Failed but with no failure cause</li>
     *   <li>{@code Stopped} → no-op (already terminal)</li>
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
                    .thenRunAsync(() -> transitionToStoppedAndClose(clusterName, lifecycle), lifecycleExecutor);
        }
        else if (state instanceof VirtualClusterLifecycleState.Draining) {
            return lifecycle.drainFuture()
                    .thenRunAsync(() -> transitionToStoppedAndClose(clusterName, lifecycle), lifecycleExecutor);
        }
        else if (state instanceof VirtualClusterLifecycleState.Stopped) {
            return CompletableFuture.completedFuture(null);
        }
        else {
            // Failed or Initializing — dispatch the whole transition+close to the lifecycle
            // thread. State is re-read inside the task; a concurrent task that beat us to
            // Stopped will cause this one to no-op.
            return CompletableFuture.runAsync(
                    () -> transitionToStoppedAndClose(clusterName, lifecycle),
                    lifecycleExecutor);
        }
    }

    /**
     * Runs on the {@link #lifecycleExecutor}. Reads the current lifecycle state and performs the
     * appropriate transition-to-{@code Stopped} plus close. Re-checking the state inside this
     * method (rather than relying on what the caller observed before dispatch) is the
     * serialization primitive: two concurrent {@code shutdownCluster} dispatches converge here,
     * and the second arrival sees {@code Stopped} and silently no-ops.
     */
    private void transitionToStoppedAndClose(String clusterName, VirtualClusterLifecycle lifecycle) {
        var current = lifecycle.state();
        if (current instanceof VirtualClusterLifecycleState.Draining) {
            lifecycle.drainComplete();
            closeAndFireStopped(clusterName, Optional.empty());
        }
        else if (current instanceof VirtualClusterLifecycleState.Failed failed) {
            lifecycle.stop();
            closeAndFireStopped(clusterName, Optional.of(failed.cause()));
        }
        else if (current instanceof VirtualClusterLifecycleState.Initializing) {
            lifecycle.stop();
            closeAndFireStopped(clusterName, Optional.empty());
        }
        // Stopped or Serving (a concurrent path beat us / changed state) — silently no-op.
    }

    /**
     * The convergence point for every transition-to-{@code Stopped} branch of
     * {@link #shutdownCluster}: closes the cluster's model (which calls
     * {@code FilterFactory.close()}) and fires the {@link #onVirtualClusterStopped} callback.
     */
    private void closeAndFireStopped(String clusterName, Optional<Throwable> failureCause) {
        closeModel(clusterName);
        onVirtualClusterStopped.accept(clusterName, failureCause);
    }

    /**
     * Closes per-VC resources (FilterChainFactory, TLS credential supplier manager) at the
     * moment the lifecycle transitions into {@code Stopped}. Called from every transition-
     * into-Stopped branch of {@link #shutdownCluster}
     *
     * <p>If the model close throws, the failure is logged at WARN and swallowed — we don't
     * want a noisy filter cleanup to stop us from firing {@link #onVirtualClusterStopped}
     * or completing the shutdown future. The exception is recorded against the cluster name
     * so operators can diagnose stuck cleanups.
     */
    private void closeModel(String clusterName) {
        var entry = entriesByCluster.get(clusterName);
        if (entry == null) {
            return;
        }
        try {
            entry.model().close();
        }
        catch (RuntimeException e) {
            LOGGER.atWarn()
                    .setCause(e)
                    .addKeyValue("virtualCluster", clusterName)
                    .addKeyValue("error", e.getMessage())
                    .log("Failed to close virtual cluster resources on lifecycle transition to Stopped");
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
     * Returns the model for the given virtual cluster name.
     *
     * @param clusterName the virtual cluster name
     * @return the model, or {@code null} if no cluster with that name exists
     */
    @Nullable
    public VirtualClusterModel modelFor(String clusterName) {
        var entry = entriesByCluster.get(clusterName);
        return entry == null ? null : entry.model();
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
     * Creates a {@link VirtualClusterLifecycle} in {@code INITIALIZING} for
     * the given model. Endpoint binding and the transition to {@code SERVING} are the
     * orchestrator's responsibility — once gateway registration succeeds it calls
     * {@link #initializationSucceeded(String)}; on failure it calls
     * {@link #initializationFailed(String, Throwable)} and rolls back the gateway bindings.
     *
     * <p>If an entry already exists for this name AND its lifecycle is {@code Stopped}, the
     * entry is replaced — this is how {@code ReplaceCluster}'s add half re-establishes the
     * cluster after the remove half drove it to {@code Stopped}. The retained-{@code Stopped}-
     * entry policy (see {@link #shutdownCluster}) interacts with re-add by name reuse, and
     * "replace the dead entry" is the natural reconciliation.
     *
     * @param newModel the model for the new cluster
     * @return an already-completed future (the operation is synchronous; the
     *         {@link CompletableFuture} shape is preserved for caller symmetry with
     *         {@link #removeVirtualCluster})
     * @throws IllegalStateException if an entry already exists AND its lifecycle is in any
     *         state OTHER than {@code Stopped} — re-adding an actively-serving (or initializing,
     *         draining, or failed) cluster would be a contract violation. The exception message
     *         names the current state to aid diagnosis.
     */
    public CompletableFuture<Void> addVirtualCluster(VirtualClusterModel newModel) {
        Objects.requireNonNull(newModel, "newModel must not be null");
        String name = newModel.getClusterName();

        entriesByCluster.compute(name, (key, existing) -> {
            if (existing != null) {
                var state = existing.lifecycle().state();
                if (!(state instanceof VirtualClusterLifecycleState.Stopped)) {
                    throw new IllegalStateException(
                            "Cluster '" + name + "' already exists and its lifecycle is "
                                    + state.getClass().getSimpleName() + "; re-add is only permitted from Stopped");
                }
            }
            LOGGER.atInfo()
                    .addKeyValue("virtualCluster", name)
                    .addKeyValue("operation", "addVirtualCluster")
                    .addKeyValue("replacingStoppedEntry", existing != null)
                    .log("reconfigure: created lifecycle in INITIALIZING; gateway registration is the orchestrator's responsibility");
            return new VirtualClusterEntry(newModel, new VirtualClusterLifecycle(name, newModel.drainTimeout()));
        });
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
